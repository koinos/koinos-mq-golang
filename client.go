package koinosmq

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	log "github.com/koinos/koinos-log-golang"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ContentType string

const (
	OctetStream = "application/octet-stream"
)

// RPCCallResult is the result of an rpc call
type RPCCallResult struct {
	Result []byte
	Error  error
}

type rpcRequest struct {
	resultChan  chan *RPCCallResult
	id          string
	contentType ContentType
	rpcService  string
	args        []byte
	expiration  string
	ctx         context.Context
}

type rpcResult struct {
	id   string
	data []byte
	err  error
}

// Client AMPQ Golang Wrapper
//
// - Each RPC message has an rpcService
// - Queue for RPC of type T is kept in queue named `koins_rpc_T`
// - RPC messages per node type
// - Single global exchange for events, all node types
type Client struct {
	/**
	 * Remote address to connect to.
	 */
	Address string

	/**
	 * Number of RPC Return consumers
	 */
	rpcReturnNumConsumers int

	rpcReturnMap map[string]chan *RPCCallResult

	rpcReplyTo     string
	rpcRetryPolicy RetryPolicy

	requestChan    chan *rpcRequest
	resultChan     chan *rpcResult
	expirationChan chan string

	conn      *connection
	connMutex sync.Mutex
}

// NewClient factory method.
func NewClient(addr string, rpcRetryPolicy RetryPolicy) *Client {
	client := &Client{
		Address:               addr,
		rpcRetryPolicy:        rpcRetryPolicy,
		rpcReturnNumConsumers: 1,
		rpcReturnMap:          make(map[string]chan *RPCCallResult),
		requestChan:           make(chan *rpcRequest, 10),
		resultChan:            make(chan *rpcResult, 10),
		expirationChan:        make(chan string, 10),
		conn:                  &connection{},
	}

	return client
}

// Start begins the connection loop. Blocks until first connected to AMQP
func (c *Client) Start(ctx context.Context) <-chan struct{} {
	connectedChan := make(chan struct{}, 1)
	go c.connectLoop(ctx, connectedChan)

	return connectedChan
}

// SetNumConsumers sets the number of consumers for queues.
//
// This sets the number of parallel goroutines that consume the respective AMQP queues.
// Must be called before Connect().
func (c *Client) SetNumConsumers(rpcReturnNumConsumers int) {
	c.rpcReturnNumConsumers = rpcReturnNumConsumers
}

func (c *Client) connectLoop(ctx context.Context, connectedChan chan<- struct{}) {
	const (
		ConnectionTimeout  = 1
		RetryMinDelay      = 1
		RetryMaxDelay      = 25
		RetryDelayPerRetry = 2
	)

	go c.connectionLoop(ctx)

	for {
		retryCount := 0
		log.Infof("Connecting client to AMQP server %v", c.Address)

		for {
			c.connMutex.Lock()

			conectCtx, connectCancel := context.WithTimeout(ctx, ConnectionTimeout*time.Second)
			defer connectCancel()

			c.conn = &connection{}
			err := c.conn.Open(conectCtx, c.Address)

			if err == nil {
				consumers, replyTo, err := c.conn.CreateRPCReturnChannels(c.rpcReturnNumConsumers)
				if err == nil {
					c.rpcReplyTo = replyTo
					for _, consumer := range consumers {
						go c.consumeRPCReturnLoop(ctx, consumer)
					}

					log.Infof("Client connected")

					if connectedChan != nil {
						connectedChan <- struct{}{}
						close(connectedChan)
						connectedChan = nil
					}

					break
				}
			}

			c.connMutex.Unlock()
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

		c.connMutex.Unlock()
		select {
		case <-c.conn.NotifyClose:
			continue
		case <-ctx.Done():
			return
		}
	}
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

func (c *Client) tryBroadcast(ctx context.Context, contentType ContentType, topic string, args []byte) error {
	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	if (c.conn == nil) || !c.conn.IsOpen() {
		return errors.New("AMQP connection is not open")
	}

	err := c.conn.AmqpChan.PublishWithContext(
		ctx,
		broadcastExchangeName,
		topic,
		false,
		false,
		amqp.Publishing{
			ContentType: string(contentType),
			Body:        args,
		},
	)

	return err
}

// Broadcast a message via AMQP
func (c *Client) Broadcast(ctx context.Context, contentType ContentType, topic string, args []byte) error {
	// Ask the retry factory for a new policy instance
	retry := getRetryPolicy(c.rpcRetryPolicy)

	for {
		// If the context has been cancelled, quit without a result
		if ctx.Err() != nil {
			return ctx.Err()
		}

		broadcastCtx, broadcastCancel := context.WithTimeout(ctx, retry.PollTimeout())
		defer broadcastCancel()

		err := c.tryBroadcast(broadcastCtx, contentType, topic, args)
		// If there were no errors, we are done
		if err == nil {
			return err
		}

		// See if the policy requests a retry
		retryResult := retry.CheckRetry()
		if !retryResult.DoRetry {
			//log.Warnf("RPC failed with error: %v", callResult.Error)
			break
		}

		// Sleep for the required amount of time
		select {
		case <-time.After(retryResult.Timeout):
		case <-ctx.Done():
		}
	}

	return errors.New("broadcast failed")
}

func (c *Client) tryRPC(ctx context.Context, contentType ContentType, rpcService string, expiration string, args []byte) ([]byte, error) {
	corrID := randomString(32)
	resultChan := make(chan *RPCCallResult, 1)

	select {
	case c.requestChan <- &rpcRequest{
		resultChan:  resultChan,
		id:          corrID,
		contentType: contentType,
		rpcService:  rpcService,
		args:        args,
		expiration:  expiration,
		ctx:         ctx,
	}:
	case <-ctx.Done():
		go func() {
			c.expirationChan <- corrID
		}()
		return nil, ctx.Err()
	}

	select {
	case res := <-resultChan:
		return res.Result, res.Error
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// RPC makes an RPC call
func (c *Client) RPC(ctx context.Context, contentType ContentType, rpcService string, args []byte) ([]byte, error) {
	// Ask the retry factory for a new policy instance
	retry := getRetryPolicy(c.rpcRetryPolicy)

	for {
		// If the context has been cancelled, quit without a result
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		rpcCtx, rpcCancel := context.WithTimeout(ctx, retry.PollTimeout())
		defer rpcCancel()

		result, err := c.tryRPC(rpcCtx, contentType, rpcService, durationToUnitString(retry.PollTimeout(), time.Millisecond), args)
		// If there were no errors, we are done
		if err == nil {
			return result, err
		}

		// See if the policy requests a retry
		retryResult := retry.CheckRetry()
		if !retryResult.DoRetry {
			//log.Warnf("RPC failed with error: %v", callResult.Error)
			break
		}

		// Sleep for the required amount of time
		select {
		case <-time.After(retryResult.Timeout):
		case <-ctx.Done():
		}
	}

	return nil, errors.New("rpc failed")
}

func (c *Client) consumeRPCReturnLoop(ctx context.Context, consumer <-chan amqp.Delivery) {
	for delivery := range consumer {
		select {
		case c.resultChan <- &rpcResult{
			id:   delivery.CorrelationId,
			data: delivery.Body,
		}:
		case <-ctx.Done():
			return
		}
	}
}

func (c *Client) handleRequest(req *rpcRequest) {
	c.connMutex.Lock()
	defer c.connMutex.Unlock()

	var err error

	if (c.conn == nil) || !c.conn.IsOpen() {
		err = errors.New("AMQP connection is not open")
	}

	if err == nil {
		c.rpcReturnMap[req.id] = req.resultChan

		err = c.conn.AmqpChan.PublishWithContext(
			req.ctx,
			rpcExchangeName,
			rpcQueuePrefix+req.rpcService,
			false,
			false,
			amqp.Publishing{
				ContentType:   string(req.contentType),
				CorrelationId: req.id,
				ReplyTo:       c.rpcReplyTo,
				Body:          req.args,
				Expiration:    req.expiration,
			},
		)
	}

	if err != nil {
		delete(c.rpcReturnMap, req.id)
		req.resultChan <- &RPCCallResult{
			Error: err,
		}
		close(req.resultChan)
	}
}

func (c *Client) handleResult(res *rpcResult) {
	if resChan, ok := c.rpcReturnMap[res.id]; ok {
		delete(c.rpcReturnMap, res.id)
		resChan <- &RPCCallResult{
			Result: res.data,
			Error:  res.err,
		}
		close(resChan)
	}
}

func (c *Client) handleExpiration(id string) {
	if resChan, ok := c.rpcReturnMap[id]; ok {
		delete(c.rpcReturnMap, id)
		resChan <- &RPCCallResult{
			Error: errors.New("rpc call timeout"),
		}
		close(resChan)
	}
}

func (c *Client) connectionLoop(ctx context.Context) {
	for {
		select {
		case req := <-c.requestChan:
			c.handleRequest(req)
		case res := <-c.resultChan:
			c.handleResult(res)
		case id := <-c.expirationChan:
			c.handleExpiration(id)

		case <-ctx.Done():
			return
		}
	}
}

func durationToUnitString(duration time.Duration, unit time.Duration) string {
	return fmt.Sprint(int(duration / unit))
}
