<?php

/**
 * This file is part of the Hollywood package.
 *
 * (c) Hollywood.com <hwwebdev@hollywood.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * @author HwWebDev <hwwebdev@hollywood.com>
 * @version 0.1
 * @package HwBundle
 */

namespace Mmoreramerino\GearmanBundle\Entity;
use Mmoreramerino\GearmanBundle\Entity\QueueStatus;
use Doctrine\ORM\Mapping as ORM;
use Doctrine\Common\Collections\ArrayCollection;
use Gedmo\Mapping\Annotation as Gedmo;
use DateTime;

/**
 * Queue Control Entity. This class is very similar to Queue, which is the default Gearman table structure, however
 * This table extends functionality adding more control over when to queue objects
 * See: http://gearman.org/index.php?id=manual:job_server
 *
 * @ORM\Entity
 * @ORM\Table(name="Queue_Control",
 *    indexes={
 *       @ORM\Index(
 *          name="NDX_Queue_Control_status_dates",
 *          columns={"queue_status_id", "create_date", "last_update_date"}
 *       ),
 *       @ORM\Index(
 *          name="NDX_Queue_Control_external_id",
 *          columns={"external_id"}
 *       )
 *       },
 *    uniqueConstraints={@ORM\UniqueConstraint(name="UNQ_hash", columns={"hash"})})
 * )
 * @ORM\HasLifecycleCallbacks
 */
class QueueControl
{

    /**
     * Id
     *
     * @ORM\Column(name="unique_key", type="string", length=64)
     * @ORM\Id
     */
    private $id;

    /**
     * Function Name, Job name. This com
     * @var string $functionName
     *
     * @ORM\Column(name="function_name", type="string", length=300, nullable=true)
     */
    private $functionName;

    /**
     * Data/Payload sent to workers
     * @var string $data
     *
     * @ORM\Column(name="data", type="text", nullable=true)
     */
    private $data;

    /**
     * Job Priority
     * @var integer $priority
     *
     * @ORM\Column(name="priority", type="integer")
     */
    private $priority;

    /**
     * Data Source,
     * @var string $dataSource
     *
     * @ORM\Column(name="data_source", type="string", length=30, nullable=true)
     */
    private $dataSource;

    /**
     * Unique hash for this job
     *
     * @var string $dataSource
     *
     * @ORM\Column(name="hash", type="string", length=100)
     */
    private $hash;

    /**
     * External Id. Set to Integer by default.
     * @var int $externalId
     *
     * @ORM\Column(name="external_id", type="integer", nullable=true)
     */
    private $externalId;

    /**
     * Parent Object
     * @var QueueControl $parent
     *
     * @ORM\ManyToOne(targetEntity="QueueControl", inversedBy="children", cascade={"all"})
     * @ORM\JoinColumn(name="parent_id", referencedColumnName="unique_key")
     */
    private $parent;

    /**
     * Children
     *
     * @var QueueControl $children
     * @ORM\OneToMany(targetEntity="QueueControl", mappedBy="parent", cascade={"all"}, fetch="LAZY")
     */
    private $children;

    /**
     * Queue Status
     * @var QueueStatus $queueStatus
     *
     * @ORM\ManyToOne(targetEntity="QueueStatus", fetch="EAGER")
     * @ORM\JoinColumns({
     *   @ORM\JoinColumn(name="queue_status_id", referencedColumnName="id", nullable=false)
     * })
     */
    private $queueStatus;

    /**
     * Retry count
     * @var int $retryCount
     *
     * @ORM\Column(name="retry_count", type="integer", nullable=true, options={"default"=0})
     */
    private $retryCount = 0;


    /**
     * Context of the job
     * @var string $executionGroup
     *
     * @ORM\Column(name="context", type="string", length=50, nullable=true)
     */
    private $context;

    /**
     * Message
     * @var string $message
     *
     * @ORM\Column(name="message", type="string", length=1000)
     */
    private $message = 'New Job';

    /**
     * Hostname
     * @var string $hostname
     *
     * @ORM\Column(name="hostname", type="string", length=100)
     */
    private $hostname;

    /**
     * Create date
     * @var DateTime $createDate
     *
     * @ORM\Column(name="create_date", type="datetime", nullable=false)
     * @Gedmo\Timestampable(on="create")
     */
    private $createDate;

    /**
     * Update Date
     * @var DateTime $createDate
     *
     * @ORM\Column(name="last_update_date", type="datetime", nullable=false)
     * @Gedmo\Timestampable(on="update" )
     */
    private $lastUpdateDate;
    /**
     * Constructor
     */
    public function __construct()
    {
        $this->children = new ArrayCollection();

        $hostname = gethostname();
        $this->hostname = ($hostname) ? $hostname : 'Unknown';
        $this->id = uuid_create();
    }
    /**
     * Get id
     *
     * @return integer
     */
    public function getId()
    {
        return $this->id;
    }

    /**
     * Set Function Name
     *
     * @param string $functionName Fucntion Name
     *
     * @return null
     *
     */
    public function setFunctionName($functionName)
    {
        $this->functionName = $functionName;
    }

    /**
     * Get functionName
     *
     * @return string
     */
    public function getFunctionName()
    {
        return $this->functionName;
    }

    /**
     * Get whenToRun
     *
     * @return string
     */
    public function getWhenToRun()
    {
        return $this->whenToRun;
    }

    /**
     * Set When to run
     *
     * @param integer $whenToRun When to run this job
     *
     */
    public function setWhenToRun($whenToRun)
    {
        $this->whenToRun = $whenToRun;
    }

    /**
     * Get priority
     *
     * @return integer
     */
    public function getPriority()
    {
        return $this->priority;
    }

    /**
     * Set When to run
     *
     * @param integer $priority Priority
     *
     */
    public function setPriority($priority)
    {
        $this->priority = $priority;
    }

    /**
     * Get data
     *
     * @return mixed
     */
    public function getData()
    {
        return unserialize($this->data);
    }

    /**
     * Set data
     *
     * @param mixed $data Data or Payload
     *
     */
    public function setData($data)
    {
        $this->data = serialize($data);
    }

    /**
     * Get dataSource
     *
     * @return string
     */
    public function getDataSource()
    {
        return $this->dataSource;
    }

    /**
     * Set dataSource
     *
     * @param integer $dataSource dataSource or Payload
     *
     */
    public function setDataSource($dataSource)
    {
        $this->dataSource = $dataSource;
    }
    /**
     * Get External Id
     * @return string
     */
    public function getExternalId()
    {
        return $this->externalId;
    }
    /**
     * Set External Id
     * @param int $externalId
     */
    public function setExternalId($externalId)
    {
        $this->externalId = $externalId;
    }
    /**
     * Get Retry count
     * @return int
     */
    public function getRetryCount()
    {
        return $this->retryCount;
    }
    /**
     * Set retry count
     * @param int $retryCount Retry counter
     */
    public function setRetryCountId($retryCount)
    {
        $this->retryCount = $retryCount;
    }

    /**
     * Gets Parent Job
     *
     * @return QueueControl
     */
    public function getParent()
    {
        return $this->parent;
    }
    /**
     * Sets Parent
     *
     * @param QueueControl $parent
     */
    public function setParent(QueueControl $parent = null)
    {
        $this->parent = $parent;
    }
    /**
     * Returns a Queue Status
     *
     * @return QueueStatus
     */
    public function getQueueStatus()
    {
        return $this->queueStatus;
    }
    /**
     * Sets QueueStatus
     *
     * @param QueueStatus $queueStatus
     */
    public function setQueueStatus(QueueStatus $queueStatus)
    {
        $this->queueStatus = $queueStatus;
    }
    /**
     * Gets Create Date
     * @return DateTime
     */
    public function getCreateDate()
    {
        return $this->createDate;
    }
    /**
     * Sets Create date
     * @param DateTime $createDate Create Date
     */
    public function setCreateDate(DateTime $createDate)
    {
        $this->createDate = $createDate;
    }
    /**
     * Gets Last update Date
     * @return DateTime
     */
    public function getLastUpdateDate()
    {
        return $this->lastUpdateDate;
    }
    /**
     * Sets Last update date
     * @param DateTime $lastUpdateDate Last update date
     */
    public function setLastUpdateDate(DateTime $lastUpdateDate)
    {
        $this->lastUpdateDate = $lastUpdateDate;
    }
    /**
     * Gets executionGroup
     * @return string
     */
    public function getExecutionGroup()
    {
        return $this->executionGroup;
    }
    /**
     * Set execution Group
     *
     * @param string $executionGroup Execution Group
     */
    public function setExecutionGroup($executionGroup)
    {
        $this->executionGroup = $executionGroup;
    }

    /**
     * Get Message attached to this Job
     *
     * @return string
     */
    public function getMessage()
    {
        return $this->message;
    }
    /**
     * Sets Message
     * @param string $message
     */
    public function setMessage($message)
    {
        $this->message = $message;
    }
    /**
     * Gets Hostname
     * @return string
     */
    public function getHostname()
    {
        return $this->hostname;
    }
    /**
     * Set Hostname
     *
     * @param string $hostname Hostname
     */
    public function setHostname($hostname)
    {
        $this->hostname = $hostname;
    }
    /**
     * Gets Hash
     * @return string
     */
    public function getHash()
    {
        return $this->hash;
    }
    /**
     * Sets Hash
     * @param string $hash
     */
    public function setHash($hash = null)
    {
        $this->hash = $hash;
    }
    /**
     * Gets all children associated with this job
     * @return ArrayCollection
     */
    public function getChildren()
    {
        return ($this->children) ?: new ArrayCollection();
    }

    /**
     * Seta All children at once
     * @param ArrayCollection $children
     */
    public function setChildren(ArrayCollection $children)
    {
        $this->children = $children;
    }
    /**
     * Adds a child to collection of children
     *
     * @param QueueControl $child
     */
    public function addChild(QueueControl $child)
    {
        $this->children->add($child);
    }

    /**
     * Context of job
     * @return string
     */
    public function getContext()
    {
        return $this->context;
    }
    /**
     * Sets job context
     * @param string $context
     */
    public function setContext($context)
    {
        $this->context = $context;
    }

    /**
     * Property setter. Calls setters useful to set fields in a loop
     * @param string $field Field
     * @param mixed  $value Value
     *
     * @return mixed
     */
    public function setField($field, $value)
    {
        return $this->{'set'.ucfirst($field)}($value);
    }
    /**
     * @ORM\PrePersist
     */
    public function generateHash()
    {
        if ($this->hash === null) {

           $this->hash = md5($this->data.$this->functionName);
        }
    }
    /**
     * Increases a retry count
     */
    public function increaseRetryCounter()
    {
        $this->retryCount++;
    }



}
