package koinosmq

import (
	"context"
	"time"

	log "github.com/koinos/koinos-log-golang"
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
	rpcHandlerMap map[string]RPCHandlerFunc

	/**
	 * Handlers for broadcast.  Indexed by topic.
	 */
	broadcastHandlerMap map[string]BroadcastHandlerFunc

	/**
	 * Number of RPC consumers
	 */
	rpcNumConsumers int

	/**
	 * Number of broadcast consumers
	 */
	broadcastNumConsumers int

	conn *connection
}

// NewRequestHandler factory method.
func NewRequestHandler(addr string) *RequestHandler {
	requestHandler := new(RequestHandler)
	requestHandler.Address = addr

	requestHandler.rpcHandlerMap = make(map[string]RPCHandlerFunc)
	requestHandler.broadcastHandlerMap = make(map[string]BroadcastHandlerFunc)

	requestHandler.rpcNumConsumers = 1
	requestHandler.broadcastNumConsumers = 1

	return requestHandler
}

// Start begins the connection loop.
func (requestHandler *RequestHandler) Start() {
	go requestHandler.ConnectLoop()
}

// SetRPCHandler sets the RPC handler for an RPC type.
func (requestHandler *RequestHandler) SetRPCHandler(rpcType string, handler RPCHandlerFunc) {
	requestHandler.rpcHandlerMap[rpcType] = handler
}

// SetBroadcastHandler sets the broadcast handler for a type.
func (requestHandler *RequestHandler) SetBroadcastHandler(topic string, handler BroadcastHandlerFunc) {
	requestHandler.broadcastHandlerMap[topic] = handler
}

// SetNumConsumers sets the number of consumers for queues.
//
// This sets the number of parallel goroutines that consume the respective AMQP queues.
// Must be called before Connect().
func (requestHandler *RequestHandler) SetNumConsumers(rpcNumConsumers int, broadcastNumConsumers int, rpcReturnNumConsumers int) {
	requestHandler.rpcNumConsumers = rpcNumConsumers
	requestHandler.broadcastNumConsumers = broadcastNumConsumers
}

// ConnectLoop is the main entry point.
func (requestHandler *RequestHandler) ConnectLoop() {
	const (
		ConnectionTimeout  = 1
		RetryMinDelay      = 1
		RetryMaxDelay      = 25
		RetryDelayPerRetry = 2
	)

	for {
		retryCount := 0
		log.Infof("Connecting to AMQP server %v", requestHandler.Address)

		for {
			requestHandler.conn = requestHandler.newConnection()
			ctx, cancel := context.WithTimeout(context.Background(), ConnectionTimeout*time.Second)
			defer cancel()
			err := requestHandler.conn.Open(ctx, requestHandler.Address)

			if err == nil {
				// Start handler consumption
				for rpcType := range requestHandler.rpcHandlerMap {
					consumers, err := requestHandler.conn.CreateRPCChannels(rpcType, requestHandler.rpcNumConsumers)
					if err != nil {
						goto Delay
					}
					for _, consumer := range consumers {
						go requestHandler.ConsumeRPCLoop(consumer, rpcType, requestHandler.conn.AmqpChan)
					}
				}

				for topic := range requestHandler.broadcastHandlerMap {
					consumers, err := requestHandler.conn.CreateBroadcastChannels(topic, requestHandler.broadcastNumConsumers)
					if err != nil {
						goto Delay
					}
					for _, consumer := range consumers {
						go requestHandler.ConsumeBroadcastLoop(consumer, topic)
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
			   case <-requestHandler.quitChan:
			      return
			*/
			case <-time.After(time.Duration(delay) * time.Second):
				retryCount++
			}
		}

		select {
		/*
		   // TODO: Add quit channel for clean termination
		   case <-requestHandler.quitChan:
		      return
		*/
		case <-requestHandler.conn.NotifyClose:
		}
	}
}

// newConnection creates a new Connection
func (requestHandler *RequestHandler) newConnection() *connection {
	conn := new(connection)
	return conn
}

// ConsumeRPCLoop consumption loop for RPC. Normally, the caller would run this function in a goroutine.
func (requestHandler *RequestHandler) ConsumeRPCLoop(consumer <-chan amqp.Delivery, rpcType string, RespChan *amqp.Channel) {
	log.Debug("Enter ConsumeRPCLoop")
	for delivery := range consumer {
		outputPub := requestHandler.HandleRPCDelivery(rpcType, &delivery)

		err := RespChan.Publish(
			rpcExchangeName,  // Exchange
			delivery.ReplyTo, // Routing key (channel name for default exchange)
			false,            // Mandatory
			false,            // Immediate
			*outputPub,       // Message
		)
		if err != nil {
			log.Errorf("Couldn't deliver message, error is %v", err)
			// TODO: Should an error close the connection?
		} else {
			delivery.Ack(true)
		}
	}
	log.Debug("Exit ConsumeRPCLoop")
}

// ConsumeBroadcastLoop consumption loop for broadcast. Normally, the caller would run this function in a goroutine.
func (requestHandler *RequestHandler) ConsumeBroadcastLoop(consumer <-chan amqp.Delivery, topic string) {
	log.Debug("Enter ConsumeBroadcastLoop")
	for delivery := range consumer {
		requestHandler.HandleBroadcastDelivery(topic, &delivery)
	}
	log.Debug("Exit ConsumeBroadcastLoop")
}

// HandleRPCDelivery handles a single RPC delivery.
//
// Parses request Delivery using ContentTypeHandler, dispath to Handler function,
// serialize response Publishing using ContentTypeHandler.
func (requestHandler *RequestHandler) HandleRPCDelivery(rpcType string, delivery *amqp.Delivery) *amqp.Publishing {
	// TODO:  Proper RPC error handling

	handler := requestHandler.rpcHandlerMap[rpcType]
	output, err := handler(rpcType, delivery.Body)
	if err != nil {
		log.Error("Error in RPC handler")
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
func (requestHandler *RequestHandler) HandleBroadcastDelivery(topic string, delivery *amqp.Delivery) {
	handler := requestHandler.broadcastHandlerMap[topic]
	handler(delivery.RoutingKey, delivery.Body)
}
