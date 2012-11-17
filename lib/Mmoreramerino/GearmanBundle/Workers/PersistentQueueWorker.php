<?php

namespace Mmoreramerino\GearmanBundle\Workers;
use Mmoreramerino\GearmanBundle\Helper\GearmanHelper;
use Mmoreramerino\GearmanBundle\Driver\Gearman;

use Symfony\Component\DependencyInjection\ContainerAwareInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;


/**
 * Generic Worker for persistent queue operations
 *
 * @Gearman\Work(iterations=5, description="Generic Worker for Persistent Queue", defaultMethod="do")
 */
class PersistentQueueWorker implements ContainerAwareInterface
{
    /**
     *
     * @var GearmanHelper $gearmanHelper
     */
    protected $gearmanHelper;

    /**
     *
     * @var ContainerInterface $container
     */
    protected $container;
    /**
     * Container
     * @param ContainerInterface $container
     */
    public function setContainer(ContainerInterface $container = null)
    {
        $this->container = $container;
        $this->gearmanHelper = $this->container->get('gearman.helper');
    }
    /**
     * Gets Gearman helper
     *
     * @return GearmanHelper
     */
    public function getGearmanHelper()
    {
        return $this->gearmanHelper;
    }
}
