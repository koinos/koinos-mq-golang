package koinosmq

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	log "github.com/koinos/koinos-log-golang"
	"github.com/streadway/amqp"
)

// RPCCallResult is the result of an rpc call
type RPCCallResult struct {
	Result []byte
	Error  error
}

// Client AMPQ Golang Wrapper
//
// - Each RPC message has an rpcType
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

	rpcReturnMap   map[string]chan rpcReturnType
	rpcReturnMutex sync.Mutex

	rpcReplyTo string

	rpcRetryPolicy RetryFactory

	conn *connection
}

type rpcReturnType struct {
	data []byte
	err  error
}

// NewClient factory method.
func NewClient(addr string, rpcRetryPolicy RetryFactory) *Client {
	client := new(Client)
	client.Address = addr

	client.rpcRetryPolicy = rpcRetryPolicy

	client.rpcReturnNumConsumers = 1

	return client
}

// Start begins the connection loop.
func (client *Client) Start() {
	go client.ConnectLoop()
}

// SetNumConsumers sets the number of consumers for queues.
//
// This sets the number of parallel goroutines that consume the respective AMQP queues.
// Must be called before Connect().
func (client *Client) SetNumConsumers(rpcReturnNumConsumers int) {
	client.rpcReturnNumConsumers = rpcReturnNumConsumers
}

// ConnectLoop is the main entry point.
func (client *Client) ConnectLoop() {
	const (
		RetryMinDelay      = 1
		RetryMaxDelay      = 25
		RetryDelayPerRetry = 2
	)

	for {
		retryCount := 0
		log.Infof("Connecting to AMQP server %v", client.Address)

		for {
			client.conn = client.newConnection()
			err := client.conn.Open(client.Address)

			if err == nil {
				consumers, replyTo, err := client.conn.CreateRPCReturnChannels(client.rpcReturnNumConsumers)
				if err != nil {
					goto Delay
				}

				client.rpcReplyTo = replyTo
				for _, consumer := range consumers {
					go client.ConsumeRPCReturnLoop(consumer)
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
			   case <-client.quitChan:
			      return
			*/
			case <-time.After(time.Duration(delay) * time.Second):
				retryCount++
			}
		}

		select {
		/*
		   // TODO: Add quit channel for clean termination
		   case <-client.quitChan:
		      return
		*/
		case <-client.conn.NotifyClose:
		}
	}
}

// newConnection creates a new Connection
func (client *Client) newConnection() *connection {
	conn := new(connection)
	client.rpcReturnMap = make(map[string]chan rpcReturnType)
	return conn
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

// Broadcast a message via AMQP
func (client *Client) Broadcast(contentType string, topic string, args []byte) error {
	err := client.conn.AmqpChan.Publish(
		broadcastExchangeName,
		topic,
		false,
		false,
		amqp.Publishing{
			ContentType: contentType,
			Body:        args,
		},
	)

	return err
}

func (client *Client) tryRPC(ctx context.Context, contentType string, rpcType string, expiration string, args []byte) *RPCCallResult {
	callResult := RPCCallResult{
		Result: nil,
		Error:  nil,
	}

	conn := client.conn

	if (conn == nil) || !conn.IsOpen() {
		callResult.Error = errors.New("AMQP connection is not open")
		return &callResult
	}

	corrID := randomString(32)
	returnChan := make(chan rpcReturnType, 1)

	client.rpcReturnMutex.Lock()
	client.rpcReturnMap[corrID] = returnChan
	client.rpcReturnMutex.Unlock()

	callResult.Error = conn.AmqpChan.Publish(
		rpcExchangeName,
		rpcQueuePrefix+rpcType,
		false,
		false,
		amqp.Publishing{
			ContentType:   contentType,
			CorrelationId: corrID,
			ReplyTo:       client.rpcReplyTo,
			Body:          args,
			Expiration:    expiration,
		},
	)

	if callResult.Error == nil {
		// Wait on channel to get result bytes or context to timeout
		select {
		case rpcResult := <-returnChan:
			if rpcResult.err != nil {
				callResult.Error = rpcResult.err
			} else {
				callResult.Result = rpcResult.data
			}
		case <-ctx.Done():
			callResult.Error = ctx.Err()
		}

	}

	client.rpcReturnMutex.Lock()
	delete(client.rpcReturnMap, corrID)
	client.rpcReturnMutex.Unlock()

	return &callResult
}

func (client *Client) makeRPCCall(ctx context.Context, contentType string, rpcType string, args []byte, done chan *RPCCallResult) {
	var callResult *RPCCallResult

	// Ask the retry factory for a new policy instance
	retry := client.rpcRetryPolicy()
	timeout := retry.PollTimeout()

	for {
		callResult = client.tryRPC(ctx, contentType, rpcType, DurationToUnitString(timeout, time.Millisecond), args)
		if callResult.Error == nil {
			break
		}

		// See if the policy requests a retry
		retryResult := retry.CheckRetry(callResult)
		if !retryResult.DoRetry {
			log.Warnf("RPC failed with error: %v", callResult.Error)
			break
		}

		// Sleep for the required amount of time
		log.Warnf("RPC error: %v", callResult.Error)
		log.Warnf("Trying again in %d seconds", int(retryResult.Timeout/time.Second))
		timeout = retryResult.Timeout
		time.Sleep(timeout)
	}

	done <- callResult
}

// RPCContext makes a block RPC call with timeout of the given context
func (client *Client) RPCContext(ctx context.Context, contentType string, rpcType string, args []byte) ([]byte, error) {
	done := make(chan *RPCCallResult)
	go client.makeRPCCall(ctx, contentType, rpcType, args, done)
	result := <-done
	return result.Result, result.Error
}

// RPC makes a blocking RPC call with no timeout
func (client *Client) RPC(contentType string, rpcType string, args []byte) ([]byte, error) {
	return client.RPCContext(context.Background(), contentType, rpcType, args)
}

// GoRPCContext asynchronously makes an RPC call with timeout of the given context
func (client *Client) GoRPCContext(ctx context.Context, contentType string, rpcType string, args []byte, done chan *RPCCallResult) {
	go client.makeRPCCall(ctx, contentType, rpcType, args, done)
}

// GoRPC asynchronously makes an RPC call with no timeout
func (client *Client) GoRPC(contentType string, rpcType string, args []byte, done chan *RPCCallResult) {
	client.GoRPCContext(context.Background(), contentType, rpcType, args, done)
}

// ConsumeRPCReturnLoop consumption loop for RPC Return. Normally, the caller would run this function in a goroutine.
func (client *Client) ConsumeRPCReturnLoop(consumer <-chan amqp.Delivery) {
	log.Debug("Enter ConsumeRPCReturnLoop")
	for delivery := range consumer {
		var result rpcReturnType
		result.data = delivery.Body
		var returnChan chan rpcReturnType
		hasReturnChan := false

		client.rpcReturnMutex.Lock()
		if val, ok := client.rpcReturnMap[delivery.CorrelationId]; ok {
			returnChan = val
			hasReturnChan = true
			delete(client.rpcReturnMap, delivery.CorrelationId)
		}
		client.rpcReturnMutex.Unlock()

		if hasReturnChan {
			returnChan <- result
		}
	}
	log.Debug("Exit ConsumeRPCReturnLoop\n")
}

// DurationToUnitString converts the given duration to an integer string in the given unit
func DurationToUnitString(duration time.Duration, unit time.Duration) string {
	return fmt.Sprint(int(duration / unit))
}
