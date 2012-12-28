<?php
/**
 * Implementation of persistent Queue
 *
 * @author Jonathan Nonon <jnonon@gmail.com>
 */
namespace Mmoreramerino\GearmanBundle\Event\Listener;

use Symfony\Component\DependencyInjection\ContainerInterface;

use Mmoreramerino\GearmanBundle\Event\JobQueueStatusChangeEvent;
use Mmoreramerino\GearmanBundle\Helper\GearmanHelper;
/**
 * Listen for changes in a job
 *
 */
class QueueStatusChangeListener
{

    private $container;
    private $gearmanHelper;
    /**
     * Constructor
     * @param ContainerInterface $container Container
     *
     */
    public function __construct(ContainerInterface $container)
    {
        $this->container = $container;

    }

    /**
     * Updates the Gearman queue with error that occurred importing content.
     *
     * @param \Hollywood\HwBundle\Event\ImportFailureEvent $event
     */
    public function onStatusChange(JobQueueStatusChangeEvent $event)
    {
        $job = $event->getJob();
        echo 'Job Changed status...'.PHP_EOL;

        if (($parent = $job->getParent()) !== null && $job->getQueueStatus()->getId() === GearmanHelper::STATUS_COMPLETED) {

            $rowsUpdated = $event->getGearmanHelper()->decreaseChildCounter($parent->getId());

            echo "I updated $rowsUpdated \n";

            //All Children are done. Ready to execute parent
            if ($rowsUpdated === 0) {
            	$event->getGearmanHelper()->enqueueJob($parent);
            	echo 'Parent Executed...'.PHP_EOL;
            }
        }
    }
}