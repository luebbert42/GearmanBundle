<?php
/**
 *
 * @author Jonathan Nonon <jnonon@gmail.com>
 *
 */
namespace Mmoreramerino\GearmanBundle\Event;
/**
 * Gearman Events
 *
 */
final class GearmanEvents
{
    /**
     * This event listen for changes on a Job
     *
     * @var string
     */
    const JOB_STATUS_CHANGED= 'gearman.event.job_status_changed';
}
