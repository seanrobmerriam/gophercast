// Package transport — TCP client for gophercast.
package transport

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gophercast/gophercast/internal/domain/message"
	"github.com/gophercast/gophercast/internal/domain/topic"
)

// reqCounter generates unique request IDs.
var reqCounter atomic.Uint64

func nextReqID() string {
	return fmt.Sprintf("req-%d", reqCounter.Add(1))
}

// response carries the server's ACK or ERROR reply to a client request.
type response struct {
	frame Frame
	err   error
}

// Client connects to a gophercast Server over TCP.
type Client struct {
	conn net.Conn
	bw   *bufio.Writer
	mu   sync.Mutex // guards bw

	pendingMu sync.Mutex
	pending   map[string]chan response // reqID → reply channel

	subsMu sync.Mutex
	subs   map[string]chan message.Message // subID → delivery channel

	closed chan struct{}
	once   sync.Once
}

// Dial connects to a gophercast server at addr and returns a ready Client.
func Dial(ctx context.Context, addr string) (*Client, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("transport: dial %s: %w", addr, err)
	}
	c := &Client{
		conn:    conn,
		bw:      bufio.NewWriter(conn),
		pending: make(map[string]chan response),
		subs:    make(map[string]chan message.Message),
		closed:  make(chan struct{}),
	}
	go c.readLoop()
	return c, nil
}

// Close closes the underlying TCP connection.
func (c *Client) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.conn.Close()
}

// Publish sends a message to the remote broker and returns delivered/dropped counts.
func (c *Client) Publish(ctx context.Context, msg message.Message) (delivered, dropped int, err error) {
	data, err := dataToRawJSON(msg.Data())
	if err != nil {
		return 0, 0, fmt.Errorf("transport: marshal data: %w", err)
	}
	reqID := nextReqID()
	f := Frame{
		Type:    FramePublish,
		ReqID:   reqID,
		Topic:   msg.Topic().String(),
		Data:    data,
		Headers: msg.Headers(),
	}
	resp, err := c.request(ctx, reqID, f)
	if err != nil {
		return 0, 0, err
	}
	if resp.frame.Type == FrameError {
		return 0, 0, fmt.Errorf("transport: publish: %s", resp.frame.Error)
	}
	return resp.frame.Delivered, resp.frame.Dropped, nil
}

// Subscribe subscribes to a topic on the remote broker.
func (c *Client) Subscribe(ctx context.Context, t topic.Topic) (*RemoteSubscription, error) {
	reqID := nextReqID()
	f := Frame{
		Type:  FrameSubscribe,
		ReqID: reqID,
		Topic: t.String(),
	}
	resp, err := c.request(ctx, reqID, f)
	if err != nil {
		return nil, err
	}
	if resp.frame.Type == FrameError {
		return nil, fmt.Errorf("transport: subscribe: %s", resp.frame.Error)
	}
	return c.newRemoteSub(ctx, resp.frame.SubscriptionID, t), nil
}

// SubscribePattern subscribes to a wildcard pattern on the remote broker.
func (c *Client) SubscribePattern(ctx context.Context, p topic.Pattern) (*RemoteSubscription, error) {
	reqID := nextReqID()
	f := Frame{
		Type:    FrameSubscribePattern,
		ReqID:   reqID,
		Pattern: p.String(),
	}
	resp, err := c.request(ctx, reqID, f)
	if err != nil {
		return nil, err
	}
	if resp.frame.Type == FrameError {
		return nil, fmt.Errorf("transport: subscribe pattern: %s", resp.frame.Error)
	}
	// Use a zero-value topic for the RemoteSubscription since it's pattern-based.
	return c.newRemoteSub(ctx, resp.frame.SubscriptionID, topic.Topic{}), nil
}

func (c *Client) newRemoteSub(ctx context.Context, subID string, t topic.Topic) *RemoteSubscription {
	ch := make(chan message.Message, 200)

	c.subsMu.Lock()
	c.subs[subID] = ch
	c.subsMu.Unlock()

	rs := &RemoteSubscription{id: subID, topic: t, msgs: ch}

	// Watch context: send UNSUBSCRIBE when ctx is cancelled.
	go func() {
		select {
		case <-ctx.Done():
		case <-c.closed:
			return
		}
		c.unsubscribe(subID)
	}()

	return rs
}

// unsubscribe tells the server to stop delivering messages for subID and
// removes the local channel.
func (c *Client) unsubscribe(subID string) {
	c.subsMu.Lock()
	ch, ok := c.subs[subID]
	if ok {
		delete(c.subs, subID)
	}
	c.subsMu.Unlock()
	if ok {
		close(ch)
	}
	// Fire-and-forget: server will also clean up when the conn closes.
	_ = c.sendFrame(Frame{
		Type:           FrameUnsubscribe,
		SubscriptionID: subID,
	})
}

// request registers a pending channel, sends f, and waits for the reply.
func (c *Client) request(ctx context.Context, reqID string, f Frame) (response, error) {
	ch := make(chan response, 1)

	c.pendingMu.Lock()
	c.pending[reqID] = ch
	c.pendingMu.Unlock()

	if err := c.sendFrame(f); err != nil {
		c.pendingMu.Lock()
		delete(c.pending, reqID)
		c.pendingMu.Unlock()
		return response{}, err
	}

	select {
	case resp := <-ch:
		return resp, resp.err
	case <-ctx.Done():
		c.pendingMu.Lock()
		delete(c.pending, reqID)
		c.pendingMu.Unlock()
		return response{}, ctx.Err()
	case <-c.closed:
		return response{}, fmt.Errorf("transport: client closed")
	}
}

func (c *Client) sendFrame(f Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := writeFrame(c.bw, f); err != nil {
		return err
	}
	return c.bw.Flush()
}

// readLoop reads frames from the server and dispatches them.
func (c *Client) readLoop() {
	br := bufio.NewReader(c.conn)
	for {
		f, err := readFrame(br)
		if err != nil {
			c.closeWithError(err)
			return
		}
		c.dispatch(f)
	}
}

func (c *Client) dispatch(f Frame) {
	switch f.Type {
	case FrameAck, FrameError:
		c.pendingMu.Lock()
		ch, ok := c.pending[f.ReqID]
		if ok {
			delete(c.pending, f.ReqID)
		}
		c.pendingMu.Unlock()
		if ok {
			ch <- response{frame: f}
		}

	case FrameMessage:
		t, _ := topic.New(f.Topic)
		hdrs := f.Headers
		if hdrs == nil {
			hdrs = map[string]string{}
		}
		ts := time.Unix(0, f.PublishedAt)
		msg := message.Reconstruct(f.MsgID, t, f.Data, hdrs, ts)

		c.subsMu.Lock()
		ch, ok := c.subs[f.SubscriptionID]
		c.subsMu.Unlock()
		if ok {
			// Non-blocking: drop if the local buffer is full.
			select {
			case ch <- msg:
			default:
			}
		}
	}
}

// closeWithError drains all pending requests with an error.
func (c *Client) closeWithError(err error) {
	c.once.Do(func() { close(c.closed) })

	c.pendingMu.Lock()
	for id, ch := range c.pending {
		ch <- response{err: fmt.Errorf("transport: connection lost: %w", err)}
		delete(c.pending, id)
	}
	c.pendingMu.Unlock()

	c.subsMu.Lock()
	for id, ch := range c.subs {
		close(ch)
		delete(c.subs, id)
	}
	c.subsMu.Unlock()
}

// RemoteSubscription is the client-side handle for a server subscription.
type RemoteSubscription struct {
	id    string
	topic topic.Topic
	msgs  chan message.Message
}

// ID returns the subscription's unique identifier.
func (rs *RemoteSubscription) ID() string { return rs.id }

// Topic returns the topic this subscription was created for.
func (rs *RemoteSubscription) Topic() topic.Topic { return rs.topic }

// MessageChannel returns the channel on which messages are delivered.
// The channel is closed when the subscription ends or the client closes.
func (rs *RemoteSubscription) MessageChannel() <-chan message.Message { return rs.msgs }
