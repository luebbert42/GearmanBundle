<?php

/**
 */
namespace Mmoreramerino\GearmanBundle\Helper;
/**
 * Gearman Helper
 *
 * @author Jonathan Nonon <jonathan.nonon@gmail.com>
 */

use Mmoreramerino\GearmanBundle\Event\Listener\QueueStatusChangeListener;
use Mmoreramerino\GearmanBundle\Event\JobQueueStatusChangeEvent;
use Mmoreramerino\GearmanBundle\Event\GearmanEvents;
use Mmoreramerino\GearmanBundle\Entity\QueueStatus;

use Doctrine\ORM\EntityManager;
use Doctrine\ORM\Query\ResultSetMappingBuilder;

use Mmoreramerino\GearmanBundle\Service\GearmanClient;
use Mmoreramerino\GearmanBundle\Entity\QueueControl;

use Symfony\Component\EventDispatcher\EventDispatcherInterface;
use Symfony\Component\DependencyInjection\ContainerAwareInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;
use PDO;
use DateTime;

/**
 * Gearman Helper
 */
class GearmanHelper extends GearmanClient
{
    //Status Constants
    const STATUS_NEW       = 1;
    const STATUS_QUEUE     = 2;
    const STATUS_WORKING   = 3;
    const STATUS_COMPLETED = 4;
    const STATUS_FAILED    = 5;
    const STATUS_WAITING   = 6;
    const STATUS_INVALID   = 7;

    //Action Result
    const ACTION_SUCESS     = 1;
    const ACTION_JOB_EXISTS = 2;
    const ACTION_FAILED     = 3;

    //Priority
    const PRIORITY_HIGH   = 2;
    const PRIORITY_NORMAL = 1;
    const PRIORITY_LOW    = 0;

    //Max Retries
    const MAX_RETRY_COUNT = 20;

    protected $priorityMap = array(self::PRIORITY_HIGH   => 'High',
                                   self::PRIORITY_NORMAL => '',
                                   self::PRIORITY_LOW    => 'Low');
    /**
     * Metadata used in persistent queue
     * @var array $expectedMetadata
     */
    private $expectedMetadata = array('context',
                                      'externalId',
                                      'dataSource');

    protected $forceToReProcess = false;

    /**
     * Host name
     * @var string $hostname
     */
    protected $hostname;
    /**
     * Container
     * @var Container $container
     */
    protected $container;
    /**
     * Entity Manager
     * @var EntityManager $entityManager
     */
    protected $entityManager;

    /**
     * Gearman Client
     * @var GearmanClient $gearmanClient
     */
    protected $gearmanClient;
    /**
     * Event Dispatcher
     *
     * @var EventDispatcherInterface $dispatcher
     */
    protected $dispatcher;

    /**
     * Deactivates parent check validation
     * @var bool $validateParentStatus
     */
    protected $validateParentStatus = true;
    /**
     * Constructor
     *
     * @param ContainerInterface       $container         Container
     * @param string                   $entityManagerName Entity manager
     * @param EventDispatcherInterface $dispatcher        Dispatcher
     */
    public function __construct(ContainerInterface $container, $entityManagerName, EventDispatcherInterface $dispatcher)
    {

        $this->container = $container;
        $this->dispatcher = $dispatcher;
        $this->entityManager = $this->container->get('doctrine')
                ->getEntityManager($entityManagerName);

        $hostname = gethostname();

        $this->hostname = ($hostname) ? $hostname : 'Unknown';
        parent::__construct($container);
    }
    /**
     * Activates/Deactivates parent check status
     *
     * @param bool  $validateParentStatus
     */
    public function setValidateParentStatus($validateParentStatus = true)
    {
        $this->validateParentStatus = (bool) $validateParentStatus;
    }
    /**
     * Sets a flag to reprocess a job item even if it is completed
     *
     * @param boolean $forceToReprocess
     */
    public function setForceToReProcess($forceToReprocess = false)
    {
        $this->forceToReProcess = (bool) $forceToReprocess;
    }

    /**
     * Gets new queued items sorted priority and creation date
     *
     * @param int $limit Limit
     *
     * @return resource
     */
    public function getNewPersistentQueuedItems($limit = 10)
    {
        //We get new items in persistent queue sorted by priority
        //TODO: Move this to Repository
        $dql = "SELECT qc
                  FROM MmoreramerinoGearmanBundle:QueueControl qc
                  WHERE qc.queueStatus = :queueStatus
                  ORDER BY qc.priority DESC, qc.lastUpdateDate DESC";

        $query = $this->entityManager->createQuery($dql)->setMaxResults($limit);
        $query->setParameters(array('queueStatus' => self::STATUS_NEW));

        return $query->getResult();
    }

    /**
     * Count Items
     * @return integer
     */
    public function getJobCountInGearmanQueue()
    {

        //TODO: Move this to Repository
        $dql = "SELECT q.functionName, COUNT(1)
                  FROM MmoreramerinoGearmanBundle:Queue q
                GROUP BY 1";

        $query = $this->entityManager->createQuery($dql);

        return $query->getSingleResult();
    }

    /**
     * Adds a job to Queue Control. All jobs submittd here are done in background
     *
     * @param string       $jobName  Job name
     * @param mixed        $data     Payload
     * @param QueueControl $parent   Parent Job
     * @param array        $metadata Metadata
     *
     * @return array
     */
    public function addNormalJobToPersistentQueue($jobName, $data, QueueControl $parent = null, array $metadata = array())
    {
        return $this->addJobToPersistentQueue($jobName, $data, self::PRIORITY_NORMAL, $parent, $metadata);
    }


    /**
     * Adds a job to Queue Control. All jobs submittd here are done in background
     *
     * @param string       $jobName  Job name
     * @param mixed        $data     Payload
     * @param QueueControl $parent   Parent Job
     * @param array        $metadata Metadata
     *
     * @return array
     */
    public function addLowJobToPersistentQueue($jobName, $data, QueueControl $parent = null, array $metadata = array())
    {
        return $this->addJobToPersistentQueue($jobName, $data, self::PRIORITY_LOW, $parent, $metadata);
    }


    /**
     * Adds a job to Queue Control. All jobs submittd here are done in background
     *
     * @param string       $jobName  Job name
     * @param mixed        $data     Payload
     * @param QueueControl $parent   Parent Job
     * @param array        $metadata Metadata
     *
     * @return array
     */
    public function addHighJobToPersistentQueue($jobName, $data, QueueControl $parent = null, array $metadata = array())
    {
        return $this->addJobToPersistentQueue($jobName, $data, self::PRIORITY_HIGH, $parent, $metadata);
    }


    /**
     * Adds a job to Queue Control. All jobs submittd here are done in background
     *
     * @param string       $jobName  Job name
     * @param mixed        $data     Payload
     * @param int          $priority Priority, Lowest is better
     * @param QueueControl $parent   Parent Job
     * @param array        $metadata Metadata
     *
     * @return array
     */
    protected function addJobToPersistentQueue($jobName, $data, $priority = 0, QueueControl $parent = null, array $metadata = array())
    {

        //Verify job has not been added to persistent queue

        $payloadSerialized = serialize($data);

        $md5 = md5($payloadSerialized.$jobName);

        $queueJob = $this->entityManager->getRepository('MmoreramerinoGearmanBundle:QueueControl')->findOneByHash($md5);

        //Assumes A failure from the begining... I am the pesismist type of programmer :-P
        $result = array('action' => self::ACTION_FAILED );

        if (!$queueJob) {
            $queueStatus = $this->getQueueStatusReference(self::STATUS_NEW);
            $queueJob =  new QueueControl();
            $queueJob->setFunctionName($jobName);
            $queueJob->setPriority($priority);
            $queueJob->setHash($md5);
            $queueJob->setData($data);
            $queueJob->setQueueStatus($queueStatus);

            foreach ($this->expectedMetadata as $property) {
                if (isset($metadata[$property])) {
                    $queueJob->setField($property, $metadata[$property]);
                }
            }
        }

        if ($parent !== null && $queueJob->getParent() === null ) {
            //This is  a litte WTF...
        	$queueJob->setParent($parent);
            $parent->addChild($queueJob);
            //END WTF
			$this->increaseChildCounter($parent->getId());
            $this->entityManager->persist($parent);

        }
        //If parent exists, set it to waiting state. Since doctrine takes care of changes,
        //A transaction maybe?
        try {


            $this->entityManager->persist($queueJob);
            $this->entityManager->flush();

        } catch (\Exception $e) {
            //Fire Events
            throw new $e;

        }

        return $queueJob;

    }

    /**
     * Increases the amount of pending jobs and children associated with a job
     * @param string $parentJobId
     *
     * @return int
     *
     */
    private function increaseChildCounter($parentJobId)
    {
		//TODO: Needs a prepare statement
    	$updateQuery = $this->entityManager->createQuery("UPDATE MmoreramerinoGearmanBundle:QueueControl qc
    			SET qc.totalChildren = qc.totalChildren + 1,
    			qc.incompleteJobs = qc.incompleteJobs + 1
    			WHERE qc.id = '$parentJobId'");

    	return $updateQuery->execute();

    }

    /**
     * Decreases the amount of pending jobs and children associated with a job
     * @param string $parentJobId
     *
     * @return int
     *
     */
    public function decreaseChildCounter($parentJobId)
    {
    	//TODO: Needs a prepare statement
    	$updateQuery = $this->entityManager->createQuery("UPDATE MmoreramerinoGearmanBundle:QueueControl qc
    			SET qc.incompleteJobs = qc.incompleteJobs - 1
    			WHERE qc.id = '$parentJobId' AND qc.incompleteJobs > 1 ");

    	return $updateQuery->execute();

    }
    /**
     * Submit jobs by function name
     *
     * @param string $functionName Job/Worker name
     * @param int    $limit        How many to submit
     *
     * @return QueueControl
     */
    public function submitJobsByFunctionName($functionName, $limit = 10)
    {
        return $this->submitJobsBy('functionName', $functionName, $limit);
    }

    /**
     * Sends a job by external Id
     *
     * @param string $externalId Job/Worker external Id
     * @return QueueControl
     */
    public function submitJobsByExternalId($externalId)
    {
        return $this->submitJobsBy('externalId', $externalId, 0);
    }
    /**
     * Sends a job by Id
     *
     * @param string $jobId Job/Worker uuid
     * @return QueueControl
     */
    public function submitJobsById($jobId)
    {
        return $this->submitJobsBy('id', $jobId, 1);
    }

    /**
     * Sends a job by Id
     *
     * @param string $jobId Job/Worker uuid
     * @return QueueControl
     */
    public function submitJobsByParent($parent)
    {
        return $this->submitJobsBy('parent', $parent, 0);
    }

    /**
     * Gets a entity Reference
     * @param integer $statusId Valid Status Id
     *
     * @return QueueStatus
     */
    private function getQueueStatusReference($statusId)
    {
        return $this->entityManager->getReference('MmoreramerinoGearmanBundle:QueueStatus', $statusId);
    }
    /**
     * Check if a job is ready to be executed. IMPORTANT: Calling this method detaches the entity ($job) from entity manager
     *
     * @param QueueControl $job Job
     * @return bool
     */
    public function isJobReadyToBeExecuted(QueueControl $job)
    {
           $children = $job->getChildren();

           echo 'I have '.count($children).' children'.PHP_EOL;

            if (empty($children)) {
                return true;
            }

            foreach ($children as $childJob) {
                $childStatus = $childJob->getQueueStatus()->getId();
                echo 'My child status is '.$childStatus.PHP_EOL;

                if (!in_array($childStatus, array(self::STATUS_COMPLETED, self::STATUS_FAILED, self::STATUS_NEW))) {
                     //Forced to query DB again by dea
                     $this->updatePersistentQueueItem($job, self::STATUS_WAITING, 'Waiting');

                     $this->entityManager->detach($job);

                     echo 'Still have incompleted jobs '.$childStatus.PHP_EOL;
                     return false;
                }
            }

            return true;



    }
    /**
     * Executes a Job
     *
     * @param QueueControl $job Job
     * @throws \Exception
     * @return bool
     */
    public function enqueueJob(QueueControl $job)
    {
        //If a job have a status waiting, it means it have children
        if (!$this->forceToReProcess && ($job->getQueueStatus()->getId() === self::STATUS_COMPLETED)) {
            $msg = 'Job was completed already';
            $this->fireStatusChangeEvent($job, self::STATUS_COMPLETED, $msg);

            echo $msg.PHP_EOL;
            return self::STATUS_COMPLETED;
        }

        $job->increaseRetryCounter();

        $methodName = 'do'.$this->priorityMap[$job->getPriority()].'BackgroundJob';
        $statusQueued = $this->getQueueStatusReference(self::STATUS_QUEUE);
        //Set Process as Working
        //TODO: Start a transaction

        try {
            echo 'Sending '.$job->getId(). ' to gearman..'.PHP_EOL;
            $this->{$methodName}($job->getFunctionName(), $job->getData(), $job->getId());

            //$datetime = new DateTime('now', new \DateTimeZone('UTC'));
            $datetime = new DateTime();

            //Queued to Gearman processing Queue
            $job->setQueueStatus($statusQueued);
            $job->setLastUpdateDate($datetime);

            $this->entityManager->persist($job);
            $this->entityManager->flush($job);

            return self::STATUS_QUEUE;

        } catch (\Exception $e) {

            echo 'Failed to Submit job';
            //TODO: Fire Event
            throw new \Exception($e);
        }

    }

    /**
     * Submits jobs by Criteria
     *
     * @param string  $field Field
     * @param mixed   $value Value to compare
     * @param integer $limit Limit of results
     */
    private function submitJobsBy($field, $value, $limit)
    {
        $statusCondition =  '';
        $parameters = array('fieldValue'  => $value);

        //If is not forced to reprocess it will require status new
        if (!$this->forceToReProcess) {
            $statusCondition = 'qc.queueStatus = :queueStatus AND ';
            $parameters['queueStatus'] = self::STATUS_NEW;
        }

        $dql = "SELECT qc
          FROM MmoreramerinoGearmanBundle:QueueControl qc
          WHERE $statusCondition
          qc.{$field} = :fieldValue
          ORDER BY qc.priority DESC, qc.lastUpdateDate DESC";


        $query = $this->entityManager->createQuery($dql);

        //Submit ALL with no limit
        if ($limit !== 0) {
            $query->setMaxResults($limit);
        }

        $query->setParameters($parameters);

        $result = $query->getResult();

        foreach ($result as $job) {

            $this->enqueueJob($job);

        }

    }


    /**
     * Updates a queue item
     *
     * @param int    $jobId    Job Id
     * @param int    $statusId Queue Status
     * @param string $msg      Message to be log
     *
     * @return mixed
     */
    public function updatePersistentQueueItemById($jobId, $statusId, $msg = '')
    {

        $job = $this->findJobById($jobId);

        if (!$job) {
            return false;
        }

        return $this->updatePersistentQueueItem($job, $statusId, $msg);

    }
    /**
     * Gets a QueueControl entity by id
     *
     * @param string $jobId Job Id
     *
     * @return mixed
     */
    public function findJobById($jobId)
    {
        //This entity may be stil in memory
        $this->entityManager->clear();

        $job = $this->entityManager->getRepository('MmoreramerinoGearmanBundle:QueueControl')->find($jobId);


        return $job;
    }


    /**
     * Updates a queue item
     *
     * @param QueueControl $job      Job
     * @param int          $statusId Queue Status
     * @param string       $msg      Message to be log
     *
     * @return QueueControl
     */
    public function updatePersistentQueueItem(QueueControl $job, $statusId, $msg = '')
    {
        $queueStatus = $this->getQueueStatusReference($statusId);
        $job->setQueueStatus($queueStatus);
        $job->setMessage($msg);

        $this->entityManager->persist($job);
        $this->entityManager->flush($job);

        $this->fireStatusChangeEvent($job, $statusId, $msg);

        return $job;
    }
    /**
     * Fires an event on status change
     *
     * @param QueueControl $job      Job
     * @param int          $statusId Status Id
     * @param string       $msg      Message
     */
    private function fireStatusChangeEvent(QueueControl $job, $statusId, $msg)
    {

        $event = new JobQueueStatusChangeEvent($this, $job, $statusId, $msg);

        $this->dispatcher->dispatch(GearmanEvents::JOB_STATUS_CHANGED, $event);

    }

    /**
     * Removes a Job from Queue
     * @param string $jobId Job Id
     *
     * @return boolean
     */
    public function removeJobById($jobId)
    {

        $job = $this->entityManager->getRepository('MmoreramerinoGearmanBundle:QueueControl')->find($jobId);

        if (!$job) {
            return false;
        }

        return $this->removeJob($job);

    }
    /**
     * Removes a Job
     *
     * @param QueueControl $job Job
     *
     * @return boolean
     */
    public function removeJob(QueueControl $job)
    {
        $jobStatusThatCanNotBeRemoved = array(self::STATUS_WAITING, self::STATUS_WORKING);

        if (in_array($job->getQueueStatus()->getId(), $jobStatusThatCanNotBeRemoved)) {
            return false;
        }

        $this->entityManager->remove($job);
        $this->entityManager->flush();

        return true;

    }

    /**
     * Tries to run pending jobs in waiting, working or queued that have been waiting for a long time
     */
    public function cleanupPersistentQueue()
    {

        $statusCheck = array(self::STATUS_WAITING => 30,
                             self::STATUS_WORKING => 15,
                             self::STATUS_QUEUE   => 15);

        $limit = 100;

        $rsm = new ResultSetMappingBuilder($this->entityManager);
        $rsm->addRootEntityFromClassMetadata('Mmoreramerino\\GearmanBundle\\Entity\\QueueControl', 'qc');
        $rsm->addJoinedEntityFromClassMetadata('Mmoreramerino\\GearmanBundle\\Entity\\QueueStatus', 'qs', 'qc', 'queueStatus', array('id' => 'queue_status_id'));

        //Query by status, making sure that it have not been a stuck on the same status for a determinated time
        $nativeSql = "SELECT qc.*, qs.*
                      FROM Queue_Control qc
                      JOIN Queue_Status qs ON qc.queue_status_id = qs.id
                      WHERE
                      (qc.queue_status_id = " .(self::STATUS_WAITING)." AND TIMESTAMPDIFF(MINUTE, qc.last_update_date, NOW() ) > {$statusCheck[self::STATUS_WAITING]}) OR
                      (qc.queue_status_id = " .(self::STATUS_WORKING)." AND TIMESTAMPDIFF(MINUTE, qc.last_update_date, NOW() ) > {$statusCheck[self::STATUS_WORKING]}) OR
                      (qc.queue_status_id = " .(self::STATUS_QUEUE)  ." AND TIMESTAMPDIFF(MINUTE, qc.last_update_date, NOW() ) > {$statusCheck[self::STATUS_QUEUE]})
                      LIMIT $limit";

        $query = $this->entityManager->createNativeQuery($nativeSql, $rsm); //->setMaxResults($limit);

        $result = $query->getResult();

        foreach ($result as $job) {
            echo $job->getId().PHP_EOL;
            if ($job->getRetryCount() >= self::MAX_RETRY_COUNT ) {
                $this->updatePersistentQueueItem($job, self::STATUS_FAILED, 'The number of retries have been max out');
                continue;
            }

            $this->enqueueJob($job);

            //$this->entityManager->detach($job);

        }


    }
    /**
     * Gets Queue size
     *
     * @return int
     */
    public function getQueueSize()
    {
        //TODO: Move this to a repository
        $dql = "SELECT COUNT(q.id) jobs FROM MmoreramerinoGearmanBundle:Queue q";

        $query = $this->entityManager->createQuery($dql);

        $result = $query->getSingleResult();

        return $result['jobs'];

    }
    /**
     * Retries failed jobs
     */
    public function retryFailedJobs($limit = 100)
    {
        $dql = "SELECT qc
                FROM MmoreramerinoGearmanBundle:QueueControl qc
                WHERE
                qc.queueStatus = :queueStatus AND
                qc.retryCount <= :retryCount AND
                qc.lastUpdateDate <=  :anHourAgo
                ORDER BY qc.priority DESC, qc.lastUpdateDate ASC";

        $anHourAgo = new DateTime();
        $anHourAgo->modify('-1 hour');

        $query = $this->entityManager->createQuery($dql)->setMaxResults($limit);

        $query->setParameters(array('queueStatus' => self::STATUS_FAILED,
                                    'retryCount'  => self::MAX_RETRY_COUNT,
                                    'anHourAgo'   => $anHourAgo));

        $result = $query->getResult();

        foreach ($result as $job) {

            $this->enqueueJob($job);
        }
    }


}