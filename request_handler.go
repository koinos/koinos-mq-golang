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

type rpcDelivery struct {
	delivery    *amqp.Delivery
	topic       string
	isBroadcast bool
}

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

	rpcHandlerMap       map[string]RPCHandlerFunc
	broadcastHandlerMap map[string]BroadcastHandlerFunc

	numConsumers uint

	deliveryChan chan *rpcDelivery

	conn *connection
}

// NewRequestHandler factory method.
func NewRequestHandler(addr string, consumers uint) *RequestHandler {
	return &RequestHandler{
		Address:             addr,
		rpcHandlerMap:       make(map[string]RPCHandlerFunc),
		broadcastHandlerMap: make(map[string]BroadcastHandlerFunc),
		numConsumers:        consumers,
		deliveryChan:        make(chan *rpcDelivery, consumers),
		conn:                &connection{},
	}
}

// Start begins the connection loop.
func (r *RequestHandler) Start(ctx context.Context) <-chan struct{} {
	connectedChan := make(chan struct{}, 1)
	go r.connectLoop(ctx, connectedChan)

	return connectedChan
}

// SetRPCHandler sets the RPC handler for an RPC type.
func (r *RequestHandler) SetRPCHandler(rpcType string, handler RPCHandlerFunc) {
	r.rpcHandlerMap[rpcType] = handler
}

// SetBroadcastHandler sets the broadcast handler for a type.
func (r *RequestHandler) SetBroadcastHandler(topic string, handler BroadcastHandlerFunc) {
	r.broadcastHandlerMap[topic] = handler
}

func (r *RequestHandler) connectLoop(ctx context.Context, connectedChan chan<- struct{}) {
	const (
		ConnectionTimeout  = 1
		RetryMinDelay      = 1
		RetryMaxDelay      = 25
		RetryDelayPerRetry = 2
	)

	for i := 0; i < int(r.numConsumers); i++ {
		go r.deliveryConsumerLoop(ctx)
	}

	for {
		retryCount := 0
		log.Infof("Connecting request handler to AMQP server %v", r.Address)

		for {
			r.conn = &connection{}
			connectCtx, connectCancel := context.WithTimeout(ctx, ConnectionTimeout*time.Second)
			defer connectCancel()
			err := r.conn.Open(connectCtx, r.Address)

			if err == nil {
				// Start handler consumption
				for rpcType := range r.rpcHandlerMap {
					consumers, err := r.conn.CreateRPCChannels(rpcType, 1)
					if err != nil {
						goto Delay
					}
					for _, consumer := range consumers {
						go r.consumeRPCLoop(ctx, consumer, rpcType, r.conn.AmqpChan)
					}
				}

				for topic := range r.broadcastHandlerMap {
					consumers, err := r.conn.CreateBroadcastChannels(topic, 1)
					if err != nil {
						goto Delay
					}
					for _, consumer := range consumers {
						go r.consumeBroadcastLoop(ctx, consumer, topic)
					}
				}
				log.Infof("Request handler connected")

				if connectedChan != nil {
					connectedChan <- struct{}{}
					close(connectedChan)
					connectedChan = nil
				}

				break
			}
		Delay:
			delay := RetryMinDelay + RetryDelayPerRetry*retryCount
			if delay > RetryMaxDelay {
				delay = RetryMaxDelay
			}

			select {
			case <-time.After(time.Duration(delay) * time.Second):
				retryCount++
			case <-ctx.Done():
				return
			}
		}

		select {
		case <-r.conn.NotifyClose:
			continue
		case <-ctx.Done():
			return
		}
	}
}

func (r *RequestHandler) consumeRPCLoop(ctx context.Context, consumer <-chan amqp.Delivery, rpcType string, RespChan *amqp.Channel) {
	for {
		select {
		case delivery, ok := <-consumer:
			if !ok {
				return
			}

			log.Debugf("Request handler received message: %v", delivery.CorrelationId)
			//fmt.Printf("Request handler received message: %v", delivery.CorrelationId)

			r.deliveryChan <- &rpcDelivery{
				delivery:    &delivery,
				isBroadcast: false,
				topic:       rpcType,
			}

		case <-ctx.Done():
			return
		}
	}
}

func (r *RequestHandler) consumeBroadcastLoop(ctx context.Context, consumer <-chan amqp.Delivery, topic string) {
	for {
		select {
		case delivery, ok := <-consumer:
			if !ok {
				return
			}

			log.Debugf("Request handler received broadcast for topic: %v", topic)
			//fmt.Printf("Request handler received message: %v", delivery.CorrelationId)

			r.deliveryChan <- &rpcDelivery{
				delivery:    &delivery,
				isBroadcast: true,
				topic:       topic,
			}

		case <-ctx.Done():
			return
		}
	}
}

func (r *RequestHandler) handleRPCDelivery(rpcType string, delivery *amqp.Delivery) {
	// TODO:  Proper RPC error handling
	var err error
	var output []byte

	if handler, ok := r.rpcHandlerMap[rpcType]; ok {
		output, err = handler(rpcType, delivery.Body)
	} else {
		log.Errorf("Could not find handler for RPC '%v'\n", rpcType)
		return
	}

	if err != nil {
		log.Errorf("Error in RPC handler, %v", err.Error())
		return
	}

	log.Debugf("Replying to message: %v", delivery.CorrelationId)

	err = r.conn.AmqpChan.Publish(
		rpcExchangeName,  // Exchange
		delivery.ReplyTo, // Routing key (channel name for default exchange)
		false,            // Mandatory
		false,            // Immediate
		amqp.Publishing{
			DeliveryMode:  amqp.Transient,
			Timestamp:     time.Now(),
			ContentType:   delivery.ContentType,
			CorrelationId: delivery.CorrelationId,
			Body:          output,
		},
	)

	if err != nil {
		log.Errorf("Couldn't deliver message, %v", err.Error())
		// TODO: Should an error close the connection?
	}
}

func (r *RequestHandler) handleBroadcastDelivery(topic string, delivery *amqp.Delivery) {
	if handler, ok := r.broadcastHandlerMap[topic]; ok {
		log.Debugf("Calling handler for %s with %v", topic, handler)
		handler(delivery.RoutingKey, delivery.Body)
	} else {
		log.Errorf("Could not find handler for Broadcast '%v'\n", topic)
		return
	}
}

func (r *RequestHandler) deliveryConsumerLoop(ctx context.Context) {
	for {
		select {
		case d := <-r.deliveryChan:
			if d.isBroadcast {
				log.Debugf("Handling broadcast for %s", d.topic)
				r.handleBroadcastDelivery(d.topic, d.delivery)
			} else {
				r.handleRPCDelivery(d.topic, d.delivery)
			}

		case <-ctx.Done():
			return
		}
	}
}
