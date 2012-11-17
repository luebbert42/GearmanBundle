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

use Doctrine\ORM\Mapping as ORM;
use Gedmo\Mapping\Annotation as Gedmo;


/**
 * Queue Entity
 *
 * @ORM\Entity
 * @ORM\Table(name="Queue",
 *    indexes={
 *       @ORM\Index(
 *          name="NDX_Queue_function_name",
 *          columns={"function_name"}
 *       ),
 * })
 */
class Queue
{

    /**
     * ID Created in persistent Queue
     * @var string $uniqueKey
     *
     * @ORM\Column(name="unique_key", type="string", length=64)
     * @ORM\OneToOne(targetEntity="QueueControl")
     * @ORM\JoinColumn(name="unique_key", referencedColumnName="unique_key")
     * @ORM\Id
     */
    private $id;

    /**
     * Function Name, Job name
     * @var string $functionName
     *
     * @ORM\Column(name="function_name", type="string", length=100, nullable=true)
     */
    private $functionName;


    /**
     * When to Run this job, currently not implemented in Gearman Client
     * @var integer $whenToRun
     *
     * @ORM\Column(name="when_to_run", type="integer")
     */
    private $whenToRun;

    /**
     * Job Priority
     * @var integer $priority
     *
     * @ORM\Column(name="priority", type="integer")
     */
    private $priority;

     /**
     * Data/Payload sent to workers
     * @var string $data
     *
     * @ORM\Column(name="data", type="text")
     */
    private $data;
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
     * @return integer
     */
    public function getData()
    {
        return $this->data;
    }

    /**
     * Set data
     *
     * @param integer $data Data or Payload
     *
     */
    public function setData($data)
    {
        $this->data = $data;
    }



}
