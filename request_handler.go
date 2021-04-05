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

	conn *connection
}

// NewRequestHandler factory method.
func NewRequestHandler(addr string) *RequestHandler {
	request_handler := new(RequestHandler)
	request_handler.Address = addr

	request_handler.RPCHandlerMap = make(map[string]RPCHandlerFunc)
	request_handler.BroadcastHandlerMap = make(map[string]BroadcastHandlerFunc)

	request_handler.RPCNumConsumers = 1
	request_handler.BroadcastNumConsumers = 1

	return request_handler
}

// Start begins the connection loop.
func (request_handler *RequestHandler) Start() {
	go request_handler.ConnectLoop()
}

// SetRPCHandler sets the RPC handler for an RPC type.
func (request_handler *RequestHandler) SetRPCHandler(rpcType string, handler RPCHandlerFunc) {
	request_handler.RPCHandlerMap[rpcType] = handler
}

// SetBroadcastHandler sets the broadcast handler for a type.
func (request_handler *RequestHandler) SetBroadcastHandler(topic string, handler BroadcastHandlerFunc) {
	request_handler.BroadcastHandlerMap[topic] = handler
}

// SetNumConsumers sets the number of consumers for queues.
//
// This sets the number of parallel goroutines that consume the respective Arequest_handlerP queues.
// Must be called before Connect().
func (request_handler *RequestHandler) SetNumConsumers(rpcNumConsumers int, broadcastNumConsumers int, rpcReturnNumConsumers int) {
	request_handler.RPCNumConsumers = rpcNumConsumers
	request_handler.BroadcastNumConsumers = broadcastNumConsumers
}

// ConnectLoop is the main entry point.
func (request_handler *RequestHandler) ConnectLoop() {
	const (
		RetryMinDelay      = 1
		RetryMaxDelay      = 25
		RetryDelayPerRetry = 2
	)

	for {
		retryCount := 0
		log.Printf("Connecting to Arequest_handlerP server %v\n", request_handler.Address)

		for {
			request_handler.conn = request_handler.newConnection()
			err := request_handler.conn.Open(request_handler.Address)
			if err == nil {
				// Start handler consumption
				for rpcType := range request_handler.RPCHandlerMap {
					consumers, err := request_handler.conn.CreateRPCChannels(rpcType, request_handler.RPCNumConsumers)
					if err != nil {
						goto Delay
					}
					for _, consumer := range consumers {
						go request_handler.ConsumeRPCLoop(consumer, rpcType, request_handler.conn.AmqpChan)
					}
				}

				for topic := range request_handler.BroadcastHandlerMap {
					consumers, err := request_handler.conn.CreateBroadcastChannels(topic, request_handler.BroadcastNumConsumers)
					if err != nil {
						goto Delay
					}
					for _, consumer := range consumers {
						go request_handler.ConsumeBroadcastLoop(consumer, topic)
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
			   case <-request_handler.quitChan:
			      return
			*/
			case <-time.After(time.Duration(delay) * time.Second):
				retryCount++
			}
		}

		select {
		/*
		   // TODO: Add quit channel for clean termination
		   case <-request_handler.quitChan:
		      return
		*/
		case <-request_handler.conn.NotifyClose:
		}
	}
}

// newConnection creates a new Connection
func (request_handler *RequestHandler) newConnection() *connection {
	conn := new(connection)
	return conn
}

// ConsumeRPCLoop consumption loop for RPC. Normally, the caller would run this function in a goroutine.
func (request_handler *RequestHandler) ConsumeRPCLoop(consumer <-chan amqp.Delivery, rpcType string, RespChan *amqp.Channel) {
	log.Printf("Enter ConsumeRPCLoop\n")
	for delivery := range consumer {
		outputPub := request_handler.HandleRPCDelivery(rpcType, &delivery)

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
func (request_handler *RequestHandler) ConsumeBroadcastLoop(consumer <-chan amqp.Delivery, topic string) {
	log.Printf("Enter ConsumeBroadcastLoop\n")
	for delivery := range consumer {
		request_handler.HandleBroadcastDelivery(topic, &delivery)
	}
	log.Printf("Exit ConsumeBroadcastLoop\n")
}

// HandleRPCDelivery handles a single RPC delivery.
//
// Parses request Delivery using ContentTypeHandler, dispath to Handler function,
// serialize response Publishing using ContentTypeHandler.
func (request_handler *RequestHandler) HandleRPCDelivery(rpcType string, delivery *amqp.Delivery) *amqp.Publishing {
	// TODO:  Proper RPC error handling

	handler := request_handler.RPCHandlerMap[rpcType]
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
func (request_handler *RequestHandler) HandleBroadcastDelivery(topic string, delivery *amqp.Delivery) {
	handler := request_handler.BroadcastHandlerMap[topic]
	handler(delivery.RoutingKey, delivery.Body)
}
