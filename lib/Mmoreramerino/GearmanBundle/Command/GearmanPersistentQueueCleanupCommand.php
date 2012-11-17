<?php

namespace Mmoreramerino\GearmanBundle\Command;

use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputDefinition;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;

/**
 * Gearman cleans up pending jobs in limbo
 * TODO: Add a explanation why this is needed
 *
 * @author jnonon <jnonon@gmail.com>
 */
class GearmanPersistentQueueCleanupCommand extends ContainerAwareCommand
{
    /**
     * Console Command configuration
     */
    protected function configure()
    {
        parent::configure();
        $this->setName('gearman:persistent-queue:cleanup')
             ->setDescription('Clean up script to submit all stuck jobs');
    }

    /**
     * Executes the current command.
     *
     * @param InputInterface  $input  An InputInterface instance
     * @param OutputInterface $output An OutputInterface instance
     *
     * @return integer 0 if everything went fine, or an error code
     *
     * @throws \LogicException When this abstract class is not implemented
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $gearmanHelper = $this->getContainer()->get('gearman.helper');

        $pendingJobs = $gearmanHelper->cleanupPersistentQueue();



    }
}