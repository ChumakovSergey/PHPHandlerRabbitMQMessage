<?php
/**
 * Created by PhpStorm.
 * User: Чумаков
 * Date: 18.06.2019
 * Time: 10:03
 */

namespace MEC;

require_once __DIR__ . '/../vendor/autoload.php';

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

class RabbitMQMessage
{
    private $result_queue;
    private $correlation_id;
    private $message;
    private $message_prop;
    private $result = null;
    private $rpc_mode;
    private $connection;
    private $errorCallbackFunc;

    public function __construct($host, $port, $user, $password, $message, $result_queue = null, $reply_to_routing_key = null, $correlation_id = null, $errorCallbackFunc = null)
    {
        $this->connection = new AMQPStreamConnection($host, $port, $user, $password);
        $this->message = $message;
        $this->result_queue = $result_queue;
        $this->correlation_id = $correlation_id;
        $this->message_prop = array('correlation_id' => $correlation_id, 'reply_to' => $reply_to_routing_key);
        $this->rpc_mode = $result_queue != null && $reply_to_routing_key != null && $this->correlation_id != null ? true : false;
        $this->errorCallbackFunc = $errorCallbackFunc;
    }

    function __destruct()
    {
        $this->connection->close();
    }

    public function Send($exchange, $routing_key)
    {
        try {
            $channel = $this->connection->channel();
            //Publishing message to MSSQL Server
            $msg = new AMQPMessage($this->message, $this->message_prop);
            $channel->basic_publish($msg, $exchange, $routing_key);
            //Setting callback function and waiting returned data
            if ($this->rpc_mode) {
                $callback = function ($msg) {
                    if ($this->correlation_id == $msg->get('correlation_id')) {
                        $this->result = $msg->body;
                        $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
                    } else {
                        $requeue = true;
                        if (isset($this->errorCallbackFunc)) {
                            $callback = $this->errorCallbackFunc;
                            $res = $callback($msg->body, $msg->get('correlation_id'), $this->correlation_id);
                            if($res)
                                $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
                        }
                        //$msg->delivery_info['channel']->basic_reject($msg->delivery_info['delivery_tag'], $requeue);
                    }

                };
                $channel->basic_consume($this->result_queue, 'php_web_site', false, false, false, false, $callback);
                //Waiting new data from queue
                try {
                    while ($this->result == null)
                        $channel->wait(null, false, 10);
                    //Since the wait() function has been executed, this means that the $callback function has been started, so $this->result has a new value
                    $result = (object)array("status" => true, "result" => $this->result);
                } catch (\PhpAmqpLib\Exception\AMQPTimeoutException $e) {
                    $result = (object)array("status" => false, "error" => "Timeout of reading MSSQL response message is out");
                }
            } else
                $result = (object)array("status" => true, "result" => $this->result);
            $channel->close();
            return $result;
        } catch (Exception $e) {
            return (object)array("status" => false, "error" => $e->getMessage());
        }
    }
}