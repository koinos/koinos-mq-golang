package koinosmq

import (
	"errors"
	"log"
	"math/rand"
	"time"

	"github.com/streadway/amqp"
)

// ContentTypeHandler Handler for a content-type.
type ContentTypeHandler interface {
	FromBytes([]byte) (interface{}, error)
	ToBytes(interface{}) ([]byte, error)
}

// RPCHandlerFunc Function type to handle an RPC message
type RPCHandlerFunc = func(rpcType string, rpc interface{}) (interface{}, error)

// BroadcastHandlerFunc Function type to handle a broadcast message
type BroadcastHandlerFunc = func(topic string, msg interface{})

// HandlerTable Transform amqp.Delivery to RpcHandler / BroadcastHandler call.
//
// This struct contains fields for purely computational dispatch and serialization logic.
type HandlerTable struct {
	/**
	 * Handlers for different content.
	 */
	ContentTypeHandlerMap map[string]ContentTypeHandler

	/**
	 * Handlers for RPC.  Indexed by rpcType.
	 */
	RPCHandlerMap map[string]RPCHandlerFunc

	/**
	 * Handlers for broadcast.  Indexed by topic.
	 */
	BroadcastHandlerMap map[string]BroadcastHandlerFunc

	/**
	 * Number of RPC consumers
	 */
	RPCNumConsumers int

	/**
	 * Number of broadcast consumers
	 */
	BroadcastNumConsumers int

	/**
	 * Number of RPC Return consumers
	 */
	RPCReturnNumConsumers int
}

const (
	broadcastExchangeName = "koinos_event"
	broadcastKeyName      = ""
	rpcExchangeName       = ""
	rpcKeyName            = ""
	rpcQueuePrefix        = "koinos_rpc_"
	rpcReplyToPrefix      = "koinos_rpc_reply_"
)

var koinosMQ *KoinosMQ

// KoinosMQ AMPQ Golang Wrapper
//
// - Each RPC message has an rpcType
// - Queue for RPC of type T is kept in queue named `koins_rpc_T`
// - RPC messages per node type
// - Single global exchange for events, all node types
type KoinosMQ struct {
	/**
	 * Remote address to connect to.
	 */
	Address string

	/**
	 * Handlers for RPC and broadcast.
	 */
	Handlers HandlerTable

	conn *connection
}

type rpcReturnType struct {
	data []byte
	err  error
}

// connection Encapsulates all the connection-specific queue.
type connection struct {
	AmqpConn *amqp.Connection
	AmqpChan *amqp.Channel

	/**
	 * RPCReceiveQueue is a durable competing-consumer queue shared by all KoinosMQ nodes.
	 */
	RPCReceiveQueue *amqp.Queue

	/**
	 * BroadcastRecvQueue is an exclusive private queue for the current KoinosMQ node.
	 * BroadcastRecvQueue will receive messages from the broadcast topic exchange.
	 */
	BroadcastRecvQueue *amqp.Queue

	/**
	 * RPCReturnQueue is an exclusive private queue for the returns of RPC calls.
	 */
	RPCReturnQueue *amqp.Queue

	/**
	 * Handlers for RPC and broadcast.
	 */
	Handlers *HandlerTable

	RPCReturnMap map[string]chan rpcReturnType

	RPCReplyTo string

	NotifyClose chan *amqp.Error
}

// NewKoinosMQ factory method.
func NewKoinosMQ(addr string) *KoinosMQ {
	if koinosMQ != nil {
		panic("KoinosMQ is a singleton and can only be created once")
	}

	mq := KoinosMQ{}
	mq.Address = addr

	mq.Handlers.ContentTypeHandlerMap = make(map[string]ContentTypeHandler)
	mq.Handlers.RPCHandlerMap = make(map[string]RPCHandlerFunc)
	mq.Handlers.BroadcastHandlerMap = make(map[string]BroadcastHandlerFunc)

	mq.Handlers.RPCNumConsumers = 1
	mq.Handlers.BroadcastNumConsumers = 1
	mq.Handlers.RPCReturnNumConsumers = 1

	koinosMQ = &mq
	return koinosMQ
}

// GetKoinosMQ returns the created singleton KoinosMQ object
func GetKoinosMQ() *KoinosMQ {
	return koinosMQ
}

// Start begins the connection loop.
func (mq *KoinosMQ) Start() {
	go mq.ConnectLoop()
}

// SetContentTypeHandler sets the content type handler for a content type.
func (mq *KoinosMQ) SetContentTypeHandler(contentType string, handler ContentTypeHandler) {
	mq.Handlers.ContentTypeHandlerMap[contentType] = handler
}

// SetRPCHandler sets the RPC handler for an RPC type.
func (mq *KoinosMQ) SetRPCHandler(rpcType string, handler RPCHandlerFunc) {
	mq.Handlers.RPCHandlerMap[rpcType] = handler
}

// SetBroadcastHandler sets the broadcast handler for a type.
func (mq *KoinosMQ) SetBroadcastHandler(topic string, handler BroadcastHandlerFunc) {
	mq.Handlers.BroadcastHandlerMap[topic] = handler
}

// SetNumConsumers sets the number of consumers for queues.
//
// This sets the number of parallel goroutines that consume the respective AMQP queues.
// Must be called before Connect().
func (mq *KoinosMQ) SetNumConsumers(rpcNumConsumers int, broadcastNumConsumers int, rpcReturnNumConsumers int) {
	mq.Handlers.RPCNumConsumers = rpcNumConsumers
	mq.Handlers.BroadcastNumConsumers = broadcastNumConsumers
	mq.Handlers.RPCReturnNumConsumers = rpcReturnNumConsumers
}

// ConnectLoop is the main entry point.
func (mq *KoinosMQ) ConnectLoop() {
	const (
		RetryMinDelay      = 1
		RetryMaxDelay      = 25
		RetryDelayPerRetry = 2
	)

	for {
		retryCount := 0
		log.Printf("Connecting to AMQP server %v\n", mq.Address)

		for {
			mq.conn = mq.newConnection()
			err := mq.conn.Open(mq.Address, &mq.Handlers)
			if err == nil {
				break
			}
			delay := RetryMinDelay + RetryDelayPerRetry*retryCount
			if delay > RetryMaxDelay {
				delay = RetryMaxDelay
			}
			select {
			/*
			   // TODO: Add quit channel for clean termination
			   case <-mq.quitChan:
			      return
			*/
			case <-time.After(time.Duration(delay) * time.Second):
				retryCount++
			}
		}
		select {
		/*
		   // TODO: Add quit channel for clean termination
		   case <-mq.quitChan:
		      return
		*/
		case <-mq.conn.NotifyClose:
		}
	}
}

// newConnection creates a new Connection
func (mq *KoinosMQ) newConnection() *connection {
	conn := connection{}
	conn.RPCReplyTo = rpcReplyToPrefix + randomString(8)
	conn.Handlers = &mq.Handlers
	return &conn
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func (mq *KoinosMQ) sendBroadcast(contentType string, args interface{}) error {
	cth := mq.Handlers.ContentTypeHandlerMap[contentType]
	bytes, err := cth.ToBytes(args)

	if err != nil {
		return err
	}

	err = mq.conn.AmqpChan.Publish(
		broadcastExchangeName,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: contentType,
			Body:        bytes,
		},
	)

	return err
}

func (mq *KoinosMQ) sendRPC(contentType string, args interface{}) (interface{}, error) {
	cth := mq.Handlers.ContentTypeHandlerMap[contentType]
	bytes, err := cth.ToBytes(args)

	if err != nil {
		return nil, err
	}

	corrID := randomString(32)
	returnChan := make(chan rpcReturnType)

	mq.conn.RPCReturnMap[corrID] = returnChan

	err = mq.conn.AmqpChan.Publish(
		"",
		"",
		false,
		false,
		amqp.Publishing{
			ContentType:   contentType,
			CorrelationId: corrID,
			ReplyTo:       mq.conn.RPCReplyTo,
			Body:          bytes,
		},
	)

	var result interface{} = nil

	if err == nil {
		// Wait on channel to get result bytes
		rpcResult := <-returnChan

		if rpcResult.err != nil {
			result, err = cth.FromBytes(rpcResult.data)
		} else {
			err = rpcResult.err
		}
	}

	delete(mq.conn.RPCReturnMap, corrID)

	return result, err
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
func (c *connection) Open(addr string, handlers *HandlerTable) error {
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
	log.Printf("Dialing AMQP server %s\n", addr)
	c.AmqpConn, err = amqp.Dial(addr)
	if err != nil {
		log.Printf("AMQP error dialing server: %v\n", err)
		return err
	}
	c.AmqpChan, err = c.AmqpConn.Channel()
	if err != nil {
		log.Printf("AMQP error connecting to channel: %v\n", err)
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
		log.Printf("AMQP error calling ExchangeDeclare: %v\n", err)
		return err
	}

	for rpcType := range handlers.RPCHandlerMap {
		consumers, err := c.ConsumeRPC(rpcType, handlers.RPCNumConsumers)
		if err != nil {
			return err
		}
		for _, consumer := range consumers {
			go c.ConsumeRPCLoop(consumer, handlers, rpcType, c.AmqpChan)
		}
	}

	for topic := range handlers.BroadcastHandlerMap {
		consumers, err := c.ConsumeBroadcast(topic, handlers.BroadcastNumConsumers)
		if err != nil {
			return err
		}
		for _, consumer := range consumers {
			go c.ConsumeBroadcastLoop(consumer, handlers, topic)
		}
	}

	consumers, err := c.ConsumeRPCReturn(handlers.RPCReturnNumConsumers)
	if err != nil {
		return err
	}
	for _, consumer := range consumers {
		go c.ConsumeRPCReturnLoop(consumer)
	}

	// TODO: Handle RPC Return expiration in separate thread

	return nil
}

// ConsumeRPC creates a delivery channel for the given RPC type.
func (c *connection) ConsumeRPC(rpcType string, numConsumers int) ([]<-chan amqp.Delivery, error) {

	rpcQueueName := "koinos_rpc_" + rpcType

	_, err := c.AmqpChan.QueueDeclare(
		rpcQueueName,
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		log.Printf("AMQP error calling QueueDeclare: %v", err)
		return nil, err
	}

	result := make([]<-chan amqp.Delivery, numConsumers)

	for i := 0; i < numConsumers; i++ {
		result[i], err = c.AmqpChan.Consume(
			rpcQueueName, // Queue
			"",           // Consumer
			false,        // AutoAck
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

// ConsumeBroadcast creates a delivery channel for the given broadcast topic.
//
// Returned channels are compteting consumers on a single AMQP queue.
func (c *connection) ConsumeBroadcast(topic string, numConsumers int) ([]<-chan amqp.Delivery, error) {

	broadcastQueue, err := c.AmqpChan.QueueDeclare(
		"",
		false, // Durable
		false, // Delete when unused
		true,  // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		log.Printf("AMQP error calling QueueDeclare: %v\n", err)
		return nil, err
	}

	result := make([]<-chan amqp.Delivery, numConsumers)

	for i := 0; i < numConsumers; i++ {
		result[i], err = c.AmqpChan.Consume(
			broadcastQueue.Name, // Queue
			"",                  // Consumer
			false,               // AutoAck
			false,               // Exclusive
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

// ConsumeRPC creates a delivery channel for the given RPC type.
func (c *connection) ConsumeRPCReturn(numConsumers int) ([]<-chan amqp.Delivery, error) {

	_, err := c.AmqpChan.QueueDeclare(
		c.RPCReplyTo,
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		log.Printf("AMQP error calling QueueDeclare: %v", err)
		return nil, err
	}

	result := make([]<-chan amqp.Delivery, numConsumers)

	for i := 0; i < numConsumers; i++ {
		result[i], err = c.AmqpChan.Consume(
			c.RPCReplyTo, // Queue
			"",           // Consumer
			false,        // AutoAck
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

// ConsumeRPCLoop consumption loop for RPC. Normally, the caller would run this function in a goroutine.
func (c *connection) ConsumeRPCLoop(consumer <-chan amqp.Delivery, handlers *HandlerTable, rpcType string, RespChan *amqp.Channel) {
	log.Printf("Enter ConsumeRPCLoop\n")
	for delivery := range consumer {
		outputPub := handlers.HandleRPCDelivery(rpcType, &delivery)

		err := RespChan.Publish(
			"",               // Exchange
			delivery.ReplyTo, // Routing key (channel name for default exchange)
			false,            // Mandatory
			false,            // Immediate
			*outputPub,       // Message
		)
		if err != nil {
			log.Printf("Couldn't deliver message, error is %v\n", err)
			// TODO: Should an error close the connection?
		}
	}
	log.Printf("Exit ConsumeRPCLoop\n")
}

// ConsumeBroadcastLoop consumption loop for broadcast. Normally, the caller would run this function in a goroutine.
func (c *connection) ConsumeBroadcastLoop(consumer <-chan amqp.Delivery, handlers *HandlerTable, topic string) {
	log.Printf("Enter ConsumeBroadcastLoop\n")
	for delivery := range consumer {
		handlers.HandleBroadcastDelivery(topic, &delivery)
	}
	log.Printf("Exit ConsumeBroadcastLoop\n")
}

// ConsumeRPCLoop consumption loop for RPC. Normally, the caller would run this function in a goroutine.
func (c *connection) ConsumeRPCReturnLoop(consumer <-chan amqp.Delivery) {
	log.Printf("Enter ConsumeRPCReturnLoop\n")
	for delivery := range consumer {
		var result rpcReturnType
		result.data = delivery.Body
		c.RPCReturnMap[delivery.CorrelationId] <- result
	}
	log.Printf("Exit ConsumeRPCReturnLoop\n")
}

// HandleRPCDelivery handles a single RPC delivery.
//
// Parses request Delivery using ContentTypeHandler, dispath to Handler function,
// serialize response Publishing using ContentTypeHandler.
func (handlers *HandlerTable) HandleRPCDelivery(rpcType string, delivery *amqp.Delivery) *amqp.Publishing {
	// TODO:  Proper RPC error handling

	cth := handlers.ContentTypeHandlerMap[delivery.ContentType]
	if cth != nil {
		log.Printf("Unknown ContentType\n")
		return nil
	}
	input, err := cth.FromBytes(delivery.Body)
	if err != nil {
		log.Printf("Couldn't deserialize rpc input\n")
		return nil
	}
	handler := handlers.RPCHandlerMap[rpcType]
	output, err := handler(rpcType, input)
	if err != nil {
		log.Printf("Error in RPC handler\n")
		return nil
	}
	outputBytes, err := cth.ToBytes(output)
	if err != nil {
		log.Printf("Couldn't serialize rpc output\n")
		return nil
	}
	outputPub := amqp.Publishing{
		DeliveryMode:  amqp.Transient,
		Timestamp:     time.Now(),
		ContentType:   delivery.ContentType,
		CorrelationId: delivery.CorrelationId,
		Body:          outputBytes,
	}
	return &outputPub
}

// HandleBroadcastDelivery handles a single broadcast delivery.
func (handlers *HandlerTable) HandleBroadcastDelivery(topic string, delivery *amqp.Delivery) {
	cth := handlers.ContentTypeHandlerMap[delivery.ContentType]
	if cth != nil {
		log.Printf("Unknown ContentType\n")
		return
	}
	input, err := cth.FromBytes(delivery.Body)
	if err != nil {
		log.Printf("Couldn't deserialize broadcast\n")
		return
	}
	handler := handlers.BroadcastHandlerMap[topic]
	handler(delivery.RoutingKey, input)
}
