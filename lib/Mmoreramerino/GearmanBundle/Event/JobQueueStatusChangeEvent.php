<?php
namespace Mmoreramerino\GearmanBundle\Event;

use Mmoreramerino\GearmanBundle\Helper\GearmanHelper;
use Mmoreramerino\GearmanBundle\Entity\QueueControl;
use Symfony\Component\EventDispatcher\Event;

/**
 * Event class used to transmit information about a failed entity import
 *
 */
class JobQueueStatusChangeEvent extends Event
{
    protected $gearmanHelper;
    protected $job;
    protected $statusId;
    protected $message;

    /**
     * Constructor
     *
     * @param GearmanHelper $gearmanHelper Gearman persistent queue
     * @param QueueControl  $job           Job that fired this event
     * @param int           $statusId      Queue status id
     * @param string        $message       Message
     */
    public function __construct(GearmanHelper $gearmanHelper, QueueControl $job, $statusId, $message)
    {

        $this->gearmanHelper = $gearmanHelper;

        $this->job           = $job;

        $this->statusId      = $statusId;

        $this->message       = $message;
    }
    /**
     * Gets Job
     *
     * @return QueueControl
     */
    public function getJob()
    {
        return $this->job;
    }
    /**
     * Gets Message
     *
     * @return string
     */
    public function getMessage()
    {
        return $this->message;
    }
    /**
     * Gets Status Id
     *
     * @return int
     */
    public function getStatusId()
    {
        return $this->statusId;
    }
    /**
     * @return GearmanHelper
     */
    public function getGearmanHelper()
    {
        return $this->gearmanHelper;
    }

}
