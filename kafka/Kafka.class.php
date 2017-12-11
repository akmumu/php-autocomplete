<?php

final class Kafka
{
    const OFFSET_BEGIN = 'beginning';
    const OFFSET_END = 'end';
    const LOG_ON = 1;//default
    const LOG_OFF = 0;
    const MODE_CONSUMER = 0;
    const MODE_PRODUCER = 1;
    const PARTITION_RANDOM = -1;
    const COMPRESSION_NONE = 'none';//default
    const COMPRESSION_GZIP = 'gzip';
    const COMPRESSION_SNAPPY = 'snappy';
    const CONFIRM_OFF = 0;
    const CONFIRM_BASIC = 1;//default
    const CONFIRM_EXTENDED = 2;//under development
    //use for $options array keys
    const RETRY_COUNT = 1;
    const RETRY_INTERVAL = 2;
    const CONFIRM_DELIVERY = 4;
    const QUEUE_BUFFER_SIZE = 8;
    const COMPRESSION_MODE = 16;
    const LOGLEVEL = 32;

    /**
     * This property does not exist, connection status
     * depends on the mode, which is stored internally
     * @var bool
     */
    private $connected = false;

    /**
     * This property was removed, instead a partition is stored
     * internally for the producer and consumer connection
     * @var int
     */
    private $partition = 0;

    /**
     * Not an actual property, but last active mode is tracked
     * This value is unreliable at this point in time, so don't rely on it...
     * @var int
     */
    private $lastMode = 0;

    /**
     * @var string
     * Internal property to track use of compression when producing messages
     */
    private $compression = self::COMPRESSION_NONE;

    /**
     * @param string $brokers
     * @param array $options
     * @throws KafkaException
     */
    public function __construct($brokers, array $options = null)
    {}

    /**
     * @param int $partition
     * @param null|int $mode
     * @deprecated use setPartition instead
     * @return $this
     * @throws \KafkaException
     */
    public function set_partition($partition, $mode = null)
    {
        if (!is_int($partition) || ($mode !== null)) {
            throw new \KafkaException('Invalid arguments passed to Kafka::set_topics');
        }
        if ($mode && $mode != self::MODE_CONSUMER && $mode != self::MODE_PRODUCER) {
            throw new \KafkaException(
                sprintf(
                    'Invalid mode passed to %s, use Kafka::MODE_* constants',
                    __METHOD__
                )
            );
        }
        if ($partition < self::PARTITION_RANDOM) {
            throw new \KafkaException('Invalid partition');
        }
        $this->partition = $partition;
        return $this;
    }

    /**
     * @param int $partition
     * @param null|$mode
     * @return $this
     * @throws \KafkaException
     */
    public function setPartition($partition, $mode = null)
    {
        if (!is_int($partition) || ($mode !== null)) {
            throw new \KafkaException('Invalid arguments passed to Kafka::set_topics');
        }
        if ($mode && $mode != self::MODE_CONSUMER && $mode != self::MODE_PRODUCER) {
            throw new \KafkaException(
                sprintf(
                    'Invalid mode passed to %s, use Kafka::MODE_* constants',
                    __METHOD__
                )
            );
        }
        if ($partition < self::PARTITION_RANDOM) {
            throw new \KafkaException('Invalid partition');
        }
        $this->partition = $partition;
        return $this;
    }

    /**
     * @param array $options
     * @throws KafkaException on invalid config
     */
    public function setOptions(array $options)
    {}

    /**
     * Note, this disconnects a previously opened producer connection!
     * @param string $compression
     * @return $this
     * @throws KafkaException
     */
    public function setCompression($compression)
    {
        if ($compression !== self::COMPRESSION_NONE && $compression !== self::COMPRESSION_GZIP && $compression !== self::COMPRESSION_SNAPPY)
            throw new KafkaException(
                sprintf('Invalid argument, use %s::COMPRESSION_* constants', __CLASS__)
            );
        $this->compression = $compression;
        return $this;
    }

    /**
     * @return string
     */
    public function getCompression()
    {
        return $this->compression;
    }

    /**
     * @param int $mode
     * @return int
     * @throws KafkaException
     */
    public function getPartition($mode)
    {
        if ($mode != self::MODE_CONSUMER && $mode != self::MODE_PRODUCER) {
            throw new \KafkaException(
                sprintf(
                    'Invalid argument passed to %s, use %s::MODE_* constants',
                    __METHOD__,
                    __CLASS__
                )
            );
        }
        return $this->partition;
    }

    /**
     * @param int $level
     * @return $this
     * @throws \KafkaException (invalid argument)
     */
    public function setLogLevel($level)
    {
        if (!is_int($level)) {
            throw new KafkaException(
                sprintf(
                    '%s expects argument to be an int',
                    __METHOD__
                )
            );
        }
        if ($level != self::LOG_ON && $level != self::LOG_OFF) {
            throw new KafkaException(
                sprintf(
                    '%s argument invalid, use %s::LOG_* constants',
                    __METHOD__,
                    __CLASS__
                )
            );
        }
        //level is passed to kafka backend
        return $this;
    }

    /**
     * @param string $brokers
     * @param array $options = null
     * @return $this
     * @throws \KafkaException
     */
    public function setBrokers($brokers, array $options = null)
    {
        if (!is_string($brokers)) {
            throw new \KafkaException(
                sprintf(
                    '%s expects argument to be a string',
                    __CLASS__
                )
            );
        }
        $this->brokers = $brokers;
        return $this;
    }

    /**
     *
     * @oaran int|null $mode
     * @return bool
     */
    public function isConnected($mode = null)
    {
        if ($mode == null) {
            $mode = $this->lastMode;
        }
        if ($mode != self::MODE_CONSUMER && $mode != self::MODE_PRODUCER) {
            throw new \KafkaException(
                sprintf(
                    'invalid argument passed to %s, use Kafka::MODE_* constants',
                    __METHOD__
                )
            );
        }
        //connection pointers determine connected status
        return $this->connected;
    }

    /**
     * produce message on topic
     * @param string $topic
     * @param string $message
     * @param int $timeout
     * @return $this
     * @throws \KafkaException
     */ 
    public function produce($topic, $message, $timeout=5000)
    {
        $this->connected = true;
        //internal call, produce message on topic
        //or throw exception
        return $this;
    }

    /**
     * Produce a batch of messages without having PHP method calls
     * Causing any overhead (internally, array is iterated, and produced
     * @param string $topic
     * @param array $messages
     * @param int $timeout
     * @return $this
     * @throws \KafkaException
     */
    public function produceBatch($topic, array $messages, $timeout=5000)
    {
        foreach ($messages as $msg) {
            //non-string messages are skipped silently ATM
            if (is_string($msg)) {
                //internally, the method call overhead is not there
                $this->produce($topic, $msg);
            }
        }
        return $this;
    }

    /**
     * @param string $topic
     * @param string|int $offset
     * @param string|int $count
     * @return array
     */
    public function consume($topic, $offset = self::OFFSET_BEGIN, $count = self::OFFSET_END)
    {
        $this->connected = true;
        $return = [];
        if (!is_numeric($offset)) {
            //0 or last message (whatever its offset might be)
            $start = $offset == self::OFFSET_BEGIN ? 0 : 100;
        } else {
            $start = $offset;
        }
        if (!is_numeric($count)) {
            //depending on amount of messages in topic
            $count = 100;
        }
        return array_fill_keys(
            range($start, $start + $count),
            'the message at the offset $key'
        );
    }

    /**
     * Returns an assoc array of topic names
     * The value is the partition count
     * @return array
     */
    public function getTopics()
    {
        return [
            'topicName' => 1
        ];
    }

    /**
     * Disconnect a specific connection (producer/consumer) or both
     * @param int|null $mode
     * @return bool
     */
    public function disconnect($mode = null)
    {
        if ($mode !== null && $mode != self::MODE_PRODUCER && $mode != self::MODE_CONSUMER) {
            throw new \KafkaException(
                sprintf(
                    'invalid argument passed to %s, use Kafka::MODE_* constants',
                    __METHOD__
                )
            );
        }
        $this->connected = false;
        return true;
    }

    /**
     * Returns an array of ints (available partitions for topic)
     * @param string $topic
     * @return array
     */
    public function getPartitionsForTopic($topic)
    {
        return [];
    }

    /**
     * Returns an array where keys are partition
     * values are their respective beginning offsets
     * if a partition has offset -1, the consume call failed
     * @param string $topic
     * @return array
     * @throws \KafkaException when meta call failed or no partitions available
     */
    public function getPartitionOffsets($topic)
    {
        return [];
    }

    public function __destruct()
    {
        $this->connected = false;
    }
}
