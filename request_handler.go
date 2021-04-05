package koinosmq

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

// RPCHandlerFunc Function type to handle an RPC message
type RPCHandlerFunc = func(rpcType string, data []byte) ([]byte, error)

// BroadcastHandlerFunc Function type to handle a broadcast message
type BroadcastHandlerFunc = func(topic string, data []byte)

// HandlerTable Transform amqp.Delivery to RpcHandler / BroadcastHandler call.
//
// This struct contains fields for purely computational dispatch and serialization logic.
type HandlerTable struct {
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
}

const (
	broadcastExchangeName = "koinos.event"
	rpcExchangeName       = "koinos.rpc"
	rpcQueuePrefix        = "koinos.rpc."
)

// RequestHandler AMPQ Golang Wrapper
//
// - Each RPC message has an rpcType
// - Queue for RPC of type T is kept in queue named `koins_rpc_T`
// - RPC messages per node type
// - Single global exchange for events, all node types
type RequestHandler struct {
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

// NewRequestHandler factory method.
func NewRequestHandler(addr string) *RequestHandler {
	mq := new(RequestHandler)
	mq.Address = addr

	mq.Handlers.RPCHandlerMap = make(map[string]RPCHandlerFunc)
	mq.Handlers.BroadcastHandlerMap = make(map[string]BroadcastHandlerFunc)

	mq.Handlers.RPCNumConsumers = 1
	mq.Handlers.BroadcastNumConsumers = 1

	return mq
}

// Start begins the connection loop.
func (mq *RequestHandler) Start() {
	go mq.ConnectLoop()
}

// SetRPCHandler sets the RPC handler for an RPC type.
func (mq *RequestHandler) SetRPCHandler(rpcType string, handler RPCHandlerFunc) {
	mq.Handlers.RPCHandlerMap[rpcType] = handler
}

// SetBroadcastHandler sets the broadcast handler for a type.
func (mq *RequestHandler) SetBroadcastHandler(topic string, handler BroadcastHandlerFunc) {
	mq.Handlers.BroadcastHandlerMap[topic] = handler
}

// SetNumConsumers sets the number of consumers for queues.
//
// This sets the number of parallel goroutines that consume the respective AMQP queues.
// Must be called before Connect().
func (mq *RequestHandler) SetNumConsumers(rpcNumConsumers int, broadcastNumConsumers int, rpcReturnNumConsumers int) {
	mq.Handlers.RPCNumConsumers = rpcNumConsumers
	mq.Handlers.BroadcastNumConsumers = broadcastNumConsumers
}

// ConnectLoop is the main entry point.
func (mq *RequestHandler) ConnectLoop() {
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
			err := mq.conn.Open(mq.Address)
			if err == nil {
				// Start handler consumption
				for rpcType := range mq.Handlers.RPCHandlerMap {
					consumers, err := mq.conn.CreateRPCChannels(rpcType, mq.Handlers.RPCNumConsumers)
					if err != nil {
						goto Delay
					}
					for _, consumer := range consumers {
						go mq.ConsumeRPCLoop(consumer, rpcType, mq.conn.AmqpChan)
					}
				}

				for topic := range mq.Handlers.BroadcastHandlerMap {
					consumers, err := mq.conn.CreateBroadcastChannels(topic, mq.Handlers.BroadcastNumConsumers)
					if err != nil {
						goto Delay
					}
					for _, consumer := range consumers {
						go mq.ConsumeBroadcastLoop(consumer, topic)
					}
				}
				break
			}
		Delay:
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
func (mq *RequestHandler) newConnection() *connection {
	conn := new(connection)
	return conn
}

// ConsumeRPCLoop consumption loop for RPC. Normally, the caller would run this function in a goroutine.
func (mq *RequestHandler) ConsumeRPCLoop(consumer <-chan amqp.Delivery, rpcType string, RespChan *amqp.Channel) {
	log.Printf("Enter ConsumeRPCLoop\n")
	for delivery := range consumer {
		outputPub := mq.Handlers.HandleRPCDelivery(rpcType, &delivery)

		err := RespChan.Publish(
			rpcExchangeName,  // Exchange
			delivery.ReplyTo, // Routing key (channel name for default exchange)
			false,            // Mandatory
			false,            // Immediate
			*outputPub,       // Message
		)
		if err != nil {
			log.Printf("Couldn't deliver message, error is %v\n", err)
			// TODO: Should an error close the connection?
		} else {
			delivery.Ack(true)
		}
	}
	log.Printf("Exit ConsumeRPCLoop\n")
}

// ConsumeBroadcastLoop consumption loop for broadcast. Normally, the caller would run this function in a goroutine.
func (mq *RequestHandler) ConsumeBroadcastLoop(consumer <-chan amqp.Delivery, topic string) {
	log.Printf("Enter ConsumeBroadcastLoop\n")
	for delivery := range consumer {
		mq.Handlers.HandleBroadcastDelivery(topic, &delivery)
	}
	log.Printf("Exit ConsumeBroadcastLoop\n")
}

// HandleRPCDelivery handles a single RPC delivery.
//
// Parses request Delivery using ContentTypeHandler, dispath to Handler function,
// serialize response Publishing using ContentTypeHandler.
func (handlers *HandlerTable) HandleRPCDelivery(rpcType string, delivery *amqp.Delivery) *amqp.Publishing {
	// TODO:  Proper RPC error handling

	handler := handlers.RPCHandlerMap[rpcType]
	output, err := handler(rpcType, delivery.Body)
	if err != nil {
		log.Printf("Error in RPC handler\n")
		return nil
	}
	outputPub := amqp.Publishing{
		DeliveryMode:  amqp.Transient,
		Timestamp:     time.Now(),
		ContentType:   delivery.ContentType,
		CorrelationId: delivery.CorrelationId,
		Body:          output,
	}
	return &outputPub
}

// HandleBroadcastDelivery handles a single broadcast delivery.
func (handlers *HandlerTable) HandleBroadcastDelivery(topic string, delivery *amqp.Delivery) {
	handler := handlers.BroadcastHandlerMap[topic]
	handler(delivery.RoutingKey, delivery.Body)
}
