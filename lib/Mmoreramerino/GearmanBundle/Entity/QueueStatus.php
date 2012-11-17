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
 * @ORM\Table(name="Queue_Status")
 */
class QueueStatus
{

    /**
     * UID Created by Gearman
     * @var varchar $uniqueKey
     *
     * @ORM\GeneratedValue(strategy="IDENTITY")
     * @ORM\Column(name="id", type="integer", nullable=false)
     * @ORM\Id
     */
    private $id;

    /**
     * Function Name, Job name
     * @var string $name
     *
     * @ORM\Column(name="name", type="string", length=100)
     */
    private $name;

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
     * Set Name
     *
     * @param string $name Name
     *
     * @return null
     *
     */
    public function setName($name)
    {
        $this->name = $name;
    }

    /**
     * Get name
     *
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }
}
