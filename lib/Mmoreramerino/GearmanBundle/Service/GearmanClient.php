<?php

namespace Mmoreramerino\GearmanBundle\Service;

use Symfony\Component\DependencyInjection\ContainerInterface;

use Mmoreramerino\GearmanBundle\Service\GearmanService;
use Mmoreramerino\GearmanBundle\Service\GearmanInterface;
use Mmoreramerino\GearmanBundle\Exceptions\NoCallableGearmanMethodException;

/**
 * Implementation of GearmanInterface
 *
 * @author Marc Morera <yuhu@mmoreram.com>
 */
class GearmanClient extends GearmanService
{

    /**
     * Construct method.
     * Performs all init actions, like initialize tasks structure
     *
     * @param ContainerInterface $container Container
     */
    public function __construct(ContainerInterface $container)
    {
        $this->setContainer($container);
        $settings = $this->loadSettings();
        $this->server = $settings['defaults']['servers'];
        $this->resetTaskStructure();
    }

    /**
     * Server variable to define in what server must connect to
     *
     * @var array
     */
    public $server = null;

    protected $client;

    /**
     * If workers are not loaded, they're loaded and returned.
     * Otherwise, they are simply returned
     *
     * @return Array Workers array getted from cache and saved
     */
    public function getWorkers()
    {
        /**
         * Always will be an Array
         */

        return $this->setWorkers();
    }

    /**
     * Runs a single task and returns some result, depending of method called.
     * Method called depends of default callable method setted on gearman settings
     *  or overwritted on work or job annotations
     *
     * @param string $name   A GearmanBundle registered function the worker is to execute
     * @param Mixed  $params Parameters to send to job
     *
     * @return mixed result depending of method called.
     */
     public function callJob($name, $params = array())
     {
        $worker = $this->getJob($name);
        $methodCallable = $worker['job']['defaultMethod'] . 'Job';

        if (!method_exists($this, $methodCallable)) {
            throw new NoCallableGearmanMethodException($methodCallable);
        }

        return $this->$methodCallable($name, $params);
     }


    /**
     * Get real worker from job name and enqueues the action given one
     *     method.
     *
     * @param string $jobName A GearmanBundle registered function the worker is to execute
     * @param mixed  $params  Parameters to send to job
     * @param string $method  Method to execute
     * @param string $unique  A unique ID used to identify a particular task
     *
     * @return mixed Return result of the call
     */
    protected function enqueue($jobName, $params, $method, $unique)
    {
        $worker = $this->getJob($jobName);
        if (false !== $worker) {
            return $this->doEnqueue($worker, $params, $method, $unique);
        }

        return false;
    }

    /**
     * Execute a GearmanClient call given a worker, params and a method.
     * If any method is given, it performs a "do" call
     *
     * If he GarmanClient call is asyncronous, result value will be a handler.
     * Otherwise, will return job result.
     *
     * @param array  $worker Worker definition
     * @param mixed  $params Parameters to send to job
     * @param string $method Method to execute
     * @param string $unique A unique ID used to identify a particular task
     *
     * @return mixed  Return result of the GearmanClient call
     */
    protected function doEnqueue(Array $worker, $params = '', $method = 'do', $unique = null)
    {
        $gmclient = new \GearmanClient();
        $this->client = $gmclient;
        $this->assignServers($gmclient);
        return $gmclient->$method($worker['job']['realCallableName'], serialize($params), $unique);
    }

    /**
     * Set server of gearman
     *
     * @param type $servername Server name (must be ip)
     * @param type $port       Port of server. By default 4730
     *
     * @return GearmanClient Returns self object
     */
    public function setServer($servername, $port = 4730)
    {
        $this->server = array($servername, $port);
        return $this;
    }

    /**
     * Given a GearmanClient, set all included servers
     *
     * @param GearmanClient $gearmanClient Object to include servers
     *
     * @return GearmanClient Returns self object
     */
    protected function assignServers(\GearmanClient $gearmanClient)
    {
        //var_dump($this->server); die();
        if (null === $this->server || !is_array($this->server)) {

            $gearmanClient->addServer();
        } else {
            foreach ($this->server as $serverName => $options) {
                $gearmanClient->addServer($options['hostname'], $options['port']);
            }
        }

        return $this;
    }

    /**
     * Clear server slot
     *
     * @return GearmanClient Returns self object
     */
    public function clearServers()
    {
        $this->server = null;

        return $this;
    }


    /**
     * Job methods
     */

    /**
     * Runs a single task and returns a string representation of the result.
     * It is up to the GearmanClient and GearmanWorker to agree on the format of the result.
     * The GearmanClient::do() method is deprecated as of pecl/gearman 1.0.0. Use GearmanClient::doNormal().
     *
     * @param string $name   A GearmanBundle registered function the worker is to execute
     * @param Mixed  $params Parameters to send to job
     * @param string $unique A unique ID used to identify a particular task
     *
     * @return string A string representing the results of running a task.
     * @deprecated
     */
    public function doJob($name, $params = array(), $unique = null)
    {

        return $this->enqueue($name, $params, 'do', $unique);
    }

    /**
     * Runs a single task and returns a string representation of the result.
     * It is up to the GearmanClient and GearmanWorker to agree on the format of the result.
     *
     * @param string $name   A GearmanBundle registered function the worker is to execute
     * @param Mixed  $params Parameters to send to job
     * @param string $unique A unique ID used to identify a particular task
     *
     * @return string A string representing the results of running a task.
     */
    public function doNormalJob($name, $params = array(), $unique = null)
    {

        return $this->enqueue($name, $params, 'doNormal', $unique);
    }


    /**
     * Runs a task in the background, returning a job handle which
     *     can be used to get the status of the running task.
     *
     * @param string $name   A GearmanBundle registered function the worker is to execute
     * @param Mixed  $params Parameters to send to job
     * @param string $unique A unique ID used to identify a particular task
     *
     * @return string Job handle for the submitted task.
     */
    public function doBackgroundJob($name, $params = array(), $unique = null)
    {

        return $this->enqueue($name, $params, 'doBackground', $unique);
    }


    /**
     * Runs a single high priority task and returns a string representation of the result.
     * It is up to the GearmanClient and GearmanWorker to agree on the format of the result.
     * High priority tasks will get precedence over normal and low priority tasks in the job queue.
     *
     * @param string $name   A GearmanBundle registered function the worker is to execute
     * @param Mixed  $params Parameters to send to job
     * @param string $unique A unique ID used to identify a particular task
     *
     * @return string A string representing the results of running a task.
     */
    public function doHighJob($name, $params = array(), $unique = null)
    {

        return $this->enqueue($name, $params, 'doHigh', $unique);
    }

    /**
     * Runs a high priority task in the background, returning a job handle which can be used to get the status of the running task.
     * High priority tasks take precedence over normal and low priority tasks in the job queue.
     *
     * @param string $name   A GearmanBundle registered function the worker is to execute
     * @param Mixed  $params Parameters to send to job
     * @param string $unique A unique ID used to identify a particular task
     *
     * @return string The job handle for the submitted task.
     */
    public function doHighBackgroundJob($name, $params = array(), $unique = null)
    {

        return $this->enqueue($name, $params, 'doHighBackground', $unique);
    }

    /**
     * Runs a single low priority task and returns a string representation of the result.
     * It is up to the GearmanClient and GearmanWorker to agree on the format of the result.
     * Normal and high priority tasks will get precedence over low priority tasks in the job queue.
     *
     * @param string $name   A GearmanBundle registered function the worker is to execute
     * @param Mixed  $params Parameters to send to job
     * @param string $unique A unique ID used to identify a particular task
     *
     * @return string A string representing the results of running a task.
     */
    public function doLowJob($name, $params = array(), $unique = null)
    {

        return $this->enqueue($name, $params, 'doLow', $unique);
    }

    /**
     * Runs a low priority task in the background, returning a job handle which can be used to get the status of the running task.
     * Normal and high priority tasks will get precedence over low priority tasks in the job queue.
     *
     * @param string $name   A GearmanBundle registered function the worker is to execute
     * @param Mixed  $params Parameters to send to job
     * @param string $unique A unique ID used to identify a particular task
     *
     * @return string The job handle for the submitted task.
     */
    public function doLowBackgroundJob($name, $params = array(), $unique = null)
    {

        return $this->enqueue($name, $params, 'doLowBackground', $unique);
    }


    /**
     * Task methods
     */

    /**
     * task structure to store all about called tasks
     *
     * @var $taskStructure
     */
    public $taskStructure = null;


    /**
     * Reset all tasks structure. Remove all set values
     *
     * @return true;
     */
    public function resetTaskStructure()
    {
        $this->taskStructure = array(
            'tasks'             =>  array(),
        );

        return true;
    }


    /**
     * Adds a task to be run in parallel with other tasks.
     * Call this method for all the tasks to be run in parallel, then call GearmanClient::runTasks() to perform the work.
     * Note that enough workers need to be available for the tasks to all run in parallel.
     *
     * @param string $name     A GermanBundle registered function to be executed
     * @param Mixed  $params   Parameters to send to task
     * @param Mixed  &$context Application context to associate with a task
     * @param string $unique   A unique ID used to identify a particular task
     *
     * @return GearmanClient Return this object
     */
    public function addTask($name, $params =array(), &$context = null, $unique = null)
    {
        $this->enqueueTask($name, $params, $context, $unique, 'addTask');

        return $this;
    }

    /**
     * Adds a high priority task to be run in parallel with other tasks.
     * Call this method for all the high priority tasks to be run in parallel, then call GearmanClient::runTasks() to perform the work.
     * Tasks with a high priority will be selected from the queue before those of normal or low priority.
     *
     * @param string $name     A GermanBundle registered function to be executed
     * @param Mixed  $params   Parameters to send to task
     * @param Mixed  &$context Application context to associate with a task
     * @param string $unique   A unique ID used to identify a particular task
     *
     * @return GearmanClient Return this object
     */
    public function addTaskHigh($name, $params =array(), &$context = null, $unique = null)
    {
        $this->enqueueTask($name, $params, $context, $unique, 'addTaskHigh');

        return $this;
    }

    /**
     * Adds a low priority background task to be run in parallel with other tasks.
     * Call this method for all the tasks to be run in parallel, then call GearmanClient::runTasks() to perform the work.
     * Tasks with a low priority will be selected from the queue after those of normal or low priority.
     *
     * @param string $name     A GermanBundle registered function to be executed
     * @param Mixed  $params   Parameters to send to task
     * @param Mixed  &$context Application context to associate with a task
     * @param string $unique   A unique ID used to identify a particular task
     *
     * @return GearmanClient Return this object
     */
    public function addTaskLow($name, $params =array(), &$context = null, $unique = null)
    {
        $this->enqueueTask($name, $params, $context, $unique, 'addTaskLow');

        return $this;
    }

    /**
     * Adds a background task to be run in parallel with other tasks
     * Call this method for all the tasks to be run in parallel, then call GearmanClient::runTasks() to perform the work.
     *
     * @param string $name     A GermanBundle registered function to be executed
     * @param Mixed  $params   Parameters to send to task
     * @param Mixed  &$context Application context to associate with a task
     * @param string $unique   A unique ID used to identify a particular task
     *
     * @return GearmanClient Return this object
     */
    public function addTaskBackground($name, $params =array(), &$context = null, $unique = null)
    {
        $this->enqueueTask($name, $params, $context, $unique, 'addTaskBackground');

        return $this;
    }

    /**
     * Adds a high priority background task to be run in parallel with other tasks.
     * Call this method for all the tasks to be run in parallel, then call GearmanClient::runTasks() to perform the work.
     * Tasks with a high priority will be selected from the queue before those of normal or low priority.
     *
     * @param string $name     A GermanBundle registered function to be executed
     * @param Mixed  $params   Parameters to send to task
     * @param Mixed  &$context Application context to associate with a task
     * @param string $unique   A unique ID used to identify a particular task
     *
     * @return GearmanClient Return this object
     */
    public function addTaskHighBackground($name, $params =array(), &$context = null, $unique = null)
    {
        $this->enqueueTask($name, $params, $context, $unique, 'addTaskHighBackground');

        return $this;
    }

    /**
     * Adds a low priority background task to be run in parallel with other tasks.
     * Call this method for all the tasks to be run in parallel, then call GearmanClient::runTasks() to perform the work.
     * Tasks with a low priority will be selected from the queue after those of normal or high priority.
     *
     * @param string $name     A GermanBundle registered function to be executed
     * @param Mixed  $params   Parameters to send to task
     * @param Mixed  &$context Application context to associate with a task
     * @param string $unique   A unique ID used to identify a particular task
     *
     * @return GearmanClient Return this object
     */
    public function addTaskLowBackground($name, $params =array(), &$context = null, $unique = null)
    {
        $this->enqueueTask($name, $params, $context, $unique, 'addTaskLowBackground');

        return $this;
    }


    /**
     * Adds a task into the structure of tasks with included type of call
     *
     * @param string $name    A GermanBundle registered function to be executed
     * @param Mixed  $params  Parameters to send to task
     * @param Mixed  $context Application context to associate with a task
     * @param string $unique  A unique ID used to identify a particular task
     * @param string $method  Method to perform
     *
     * @return GearmanClient Return this object
     */
    protected function enqueueTask($name, $params, $context, $unique, $method)
    {
        $task = array(
            'name'      =>  $name,
            'params'    =>  $params,
            'context'   =>  $context,
            'unique'    =>  $unique,
            'method'    =>  $method,
            );
        $this->addTaskToStructure($task);

        return $this;
    }

    /**
     * Appends a task structure into taskStructure array
     *
     * @param array $task Task structure
     *
     * @return GearmanClient Return this object
     */
    protected function addTaskToStructure(array $task)
    {
        $this->taskStructure['tasks'][] = $task;

        return $this;
    }


    /**
     * For a set of tasks previously added with GearmanClient::addTask(), GearmanClient::addTaskHigh(),
     * GearmanClient::addTaskLow(), GearmanClient::addTaskBackground(), GearmanClient::addTaskHighBackground(),
     * or GearmanClient::addTaskLowBackground(), this call starts running the tasks in parallel.
     * Note that enough workers need to be available for the tasks to all run in parallel
     *
     * @return true
     */
    public function runTasks()
    {
        $taskStructure = $this->taskStructure;
        $gearmanClient = new \GearmanClient();
        $this->client = $gearmanClient;
        $this->assignServers($gearmanClient);

        foreach ($taskStructure['tasks'] as $task) {
            $type = $task['method'];
            $jobName = $task['name'];
            $worker = $this->getJob($jobName);
            if (false !== $worker) {
                $gearmanClient->$type($worker['job']['realCallableName'], serialize($task['params']), $task['context'], $task['unique']);
            }
        }

        return $gearmanClient->runTasks();
    }

    /**
     * get status from Gearman, returns GEARMAN_COULD_NOT_CONNECT (26) if
     * @return array | null
     */
    public function getGearmanStatus(){
        $settings = $this->loadSettings();
        $this->server = $settings['defaults']['servers'];
        $statusList = array();
        foreach ($this->server as $server) {
            $statusList[$server["hostname"].":".$server["port"]] = $this->getStatusFromSingleServer($server["hostname"],$server["port"]);
        }
        return $statusList;
    }

    /**
     * gets the status of a given server, returns GEARMAN_COULD_NOT_CONNECT (26) if Gearman is not available
     * @param $hostname
     * @param $port
     * @return int|null
     */
    public function getStatusFromSingleServer($hostname, $port) {
        $handle = @fsockopen($hostname,$port,$errorNumber,$errorString,30);
        if($handle === false){
            return GEARMAN_COULD_NOT_CONNECT;
        }
        $status = null;
        fwrite($handle,"status\n");
        while (!feof($handle)) {
            $line = fgets($handle, 4096);
            if( $line==".\n"){
                break;
            }
            if( preg_match("~^(.*)[ \t](\d+)[ \t](\d+)[ \t](\d+)~",$line,$matches) ){
                $function = $matches[1];
                $status['operations'][$function] = array(
                    'function' => $function,
                    'total' => $matches[2],
                    'running' => $matches[3],
                    'connectedWorkers' => $matches[4],
                );
            }
        }
        fwrite($handle,"workers\n");
        while (!feof($handle)) {
            $line = fgets($handle, 4096);
            if( $line==".\n"){
                break;
            }
            // FD IP-ADDRESS CLIENT-ID : FUNCTION
            if( preg_match("~^(\d+)[ \t](.*?)[ \t](.*?) : ?(.*)~",$line,$matches) ){
                $fd = $matches[1];
                $status['connections'][$fd] = array(
                    'fd' => $fd,
                    'ip' => $matches[2],
                    'id' => $matches[3],
                    'function' => $matches[4],
                );
            }
        }
       fclose($handle);
       return $status;
    }


    /**
     * Gets the status of a job
     *
     * @param JobHandler $jobHandler Gearman Job handles
     * @return mixed
     */
    public function getJobStatus($jobHandler)
    {
        if (null == $this->client) {
            $gmclient = new \GearmanClient();
            $this->client = $gmclient;
            $this->assignServers($this->client);
        }
        return $this->client->jobStatus($jobHandler);
    }
}
