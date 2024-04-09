package koinosmq

import (
	"context"
	"errors"

	log "github.com/koinos/koinos-log-golang/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

// connection Encapsulates all the connection-specific queue.
type connection struct {
	AmqpConn *amqp.Connection
	AmqpChan *amqp.Channel

	/**
	 * RPCReceiveQueue is a durable competing-consumer queue shared by all AMQP nodes.
	 */
	RPCReceiveQueue *amqp.Queue

	/**
	 * BroadcastRecvQueue is an exclusive private queue for the current AMQP node.
	 * BroadcastRecvQueue will receive messages from the broadcast topic exchange.
	 */
	BroadcastRecvQueue *amqp.Queue

	/**
	 * RPCReturnQueue is an exclusive private queue for the returns of RPC calls.
	 */
	RPCReturnQueue *amqp.Queue

	NotifyClose chan *amqp.Error
}

/**
 * Set all fields to nil or default values.
 */
func (c *connection) reset() {
	c.AmqpConn = nil
	c.AmqpChan = nil
}

// Close the connection.
func (c *connection) Close() error {
	amqpConn := c.AmqpConn
	c.reset()

	if amqpConn == nil {
		return nil
	}
	return amqpConn.Close()
}

// Open and attempt to connection.
//
// Return error if connection attempt fails (i.e., no retry).
func (c *connection) Open(ctx context.Context, addr string) error {
	doneChan := make(chan error, 1)
	go func() {
		doneChan <- c.openImpl(addr)
	}()

	select {
	case err := <-doneChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *connection) openImpl(addr string) error {
	if (c.AmqpConn != nil) || (c.AmqpChan != nil) {
		return errors.New("Attempted to reuse connection")
	}
	var err error = nil

	closeIfError := func() {
		if err != nil {
			c.Close()
		}
	}
	defer closeIfError()

	// We keep the connection and channel local until the connection's fully set up.
	log.Infof("Dialing AMQP server %s", addr)
	c.AmqpConn, err = amqp.Dial(addr)
	if err != nil {
		log.Warnf("AMQP error dialing server: %v", err)
		return err
	}
	c.AmqpChan, err = c.AmqpConn.Channel()
	if err != nil {
		log.Warnf("AMQP error connecting to channel: %v", err)
		return err
	}

	c.NotifyClose = make(chan *amqp.Error)
	c.AmqpChan.NotifyClose(c.NotifyClose)

	err = c.AmqpChan.ExchangeDeclare(
		broadcastExchangeName, // Name
		"topic",               // type
		true,                  // durable
		false,                 // auto-deleted
		false,                 // internal
		false,                 // no-wait
		nil,                   // arguments
	)
	if err != nil {
		log.Warnf("AMQP error calling ExchangeDeclare: %v", err)
		return err
	}

	err = c.AmqpChan.ExchangeDeclare(
		rpcExchangeName, // Name
		"direct",        // type
		true,            // durable
		false,           // auto-deleted
		false,           // internal
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		log.Warnf("AMQP error calling ExchangeDeclare: %v", err)
		return err
	}

	// TODO: Handle RPC Return expiration in separate thread

	return nil
}

func (c *connection) IsOpen() bool {
	return c.AmqpConn != nil && c.AmqpChan != nil
}

// CreateRPCChannels creates a delivery channel for the given RPC type.
func (c *connection) CreateRPCChannels(rpcType string, numConsumers int) ([]<-chan amqp.Delivery, error) {

	rpcQueueName := rpcQueuePrefix + rpcType

	rpcQueue, err := c.AmqpChan.QueueDeclare(
		rpcQueueName,
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		log.Warnf("AMQP error calling QueueDeclare: %v", err)
		return nil, err
	}

	err = c.AmqpChan.QueueBind(
		rpcQueue.Name,
		rpcQueue.Name,
		rpcExchangeName,
		false,
		nil,
	)
	if err != nil {
		log.Warnf("AMQP error calling QueueBind: %v", err)
		return nil, err
	}

	result := make([]<-chan amqp.Delivery, numConsumers)

	for i := 0; i < numConsumers; i++ {
		result[i], err = c.AmqpChan.Consume(
			rpcQueueName, // Queue
			"",           // Consumer
			true,         // AutoAck
			false,        // Exclusive
			false,        // NoLocal
			false,        // NoWait
			nil,          // Arguments
		)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// CreateBroadcastChannels creates a delivery channel for the given broadcast topic.
//
// Returned channels are compteting consumers on a single AMQP queue.
func (c *connection) CreateBroadcastChannels(topic string, numConsumers int) ([]<-chan amqp.Delivery, error) {

	broadcastQueue, err := c.AmqpChan.QueueDeclare(
		"",
		false, // Durable
		true,  // Delete when unused
		true,  // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		log.Warnf("AMQP error calling QueueDeclare: %v", err)
		return nil, err
	}

	err = c.AmqpChan.QueueBind(
		broadcastQueue.Name,
		topic,
		broadcastExchangeName,
		false,
		nil,
	)
	if err != nil {
		log.Warnf("AMQP error calling QueueBind: %v", err)
		return nil, err
	}

	result := make([]<-chan amqp.Delivery, numConsumers)

	for i := 0; i < numConsumers; i++ {
		result[i], err = c.AmqpChan.Consume(
			broadcastQueue.Name, // Queue
			"",                  // Consumer
			true,                // AutoAck
			true,                // Exclusive
			false,               // NoLocal
			false,               // NoWait
			nil,                 // Arguments
		)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// CreateRPCReturnChannels creates a channels for the given RPC returns.
func (c *connection) CreateRPCReturnChannels(numConsumers int) ([]<-chan amqp.Delivery, string, error) {

	queue, err := c.AmqpChan.QueueDeclare(
		"",
		false, // Durable
		true,  // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		log.Warnf("AMQP error calling QueueDeclare: %v", err)
		return nil, "", err
	}

	err = c.AmqpChan.QueueBind(
		queue.Name,
		queue.Name,
		rpcExchangeName,
		false,
		nil,
	)
	if err != nil {
		log.Warnf("AMQP error calling QueueBind: %v", err)
		return nil, "", err
	}

	result := make([]<-chan amqp.Delivery, numConsumers)

	for i := 0; i < numConsumers; i++ {
		result[i], err = c.AmqpChan.Consume(
			queue.Name, // Queue
			"",         // Consumer
			true,       // AutoAck
			false,      // Exclusive
			false,      // NoLocal
			false,      // NoWait
			nil,        // Arguments
		)
		if err != nil {
			return nil, "", err
		}
	}

	return result, queue.Name, nil
}
