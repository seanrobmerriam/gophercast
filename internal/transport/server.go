// Package transport — TCP server that exposes a *broker.Broker over the wire.
package transport

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/gophercast/gophercast/internal/domain/broker"
	"github.com/gophercast/gophercast/internal/domain/message"
	"github.com/gophercast/gophercast/internal/domain/subscription"
	"github.com/gophercast/gophercast/internal/domain/topic"
)

// Server exposes a *broker.Broker over a TCP listener.
type Server struct {
	b        *broker.Broker
	listener net.Listener

	mu   sync.Mutex
	once sync.Once
	done chan struct{}
}

// NewServer creates a Server backed by b.
func NewServer(b *broker.Broker) *Server {
	return &Server{
		b:    b,
		done: make(chan struct{}),
	}
}

// Listen binds to addr, starts accepting connections in a background goroutine,
// and returns the actual listening address (useful when addr is ":0").
// Call Close to stop the server.
func (s *Server) Listen(addr string) (net.Addr, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("transport: server listen %s: %w", addr, err)
	}
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()
	go s.serve(ln)
	return ln.Addr(), nil
}

// Close stops the server by closing the underlying listener.
func (s *Server) Close() error {
	s.once.Do(func() { close(s.done) })
	s.mu.Lock()
	ln := s.listener
	s.mu.Unlock()
	if ln != nil {
		return ln.Close()
	}
	return nil
}

func (s *Server) serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.done:
				return nil
			default:
				return fmt.Errorf("transport: accept: %w", err)
			}
		}
		go s.handleConn(conn)
	}
}

// session is the per-connection state.
type session struct {
	b    *broker.Broker
	conn net.Conn
	bw   *bufio.Writer
	mu   sync.Mutex // guards bw

	ctx    context.Context
	cancel context.CancelFunc

	subsMu sync.Mutex
	subs   map[string]context.CancelFunc // subID → cancel
}

func (s *Server) handleConn(conn net.Conn) {
	ctx, cancel := context.WithCancel(context.Background())
	sess := &session{
		b:      s.b,
		conn:   conn,
		bw:     bufio.NewWriter(conn),
		ctx:    ctx,
		cancel: cancel,
		subs:   make(map[string]context.CancelFunc),
	}
	defer func() {
		cancel() // cascade-closes all subscriptions started by this session
		conn.Close()
	}()

	br := bufio.NewReader(conn)
	for {
		f, err := readFrame(br)
		if err != nil {
			// Connection closed or broken — normal exit.
			return
		}
		sess.dispatch(f)
	}
}

func (sess *session) dispatch(f Frame) {
	switch f.Type {
	case FramePublish:
		sess.handlePublish(f)
	case FrameSubscribe:
		sess.handleSubscribe(f)
	case FrameSubscribePattern:
		sess.handleSubscribePattern(f)
	case FrameUnsubscribe:
		sess.handleUnsubscribe(f)
	default:
		sess.sendError(f.ReqID, fmt.Sprintf("unknown frame type %q", f.Type))
	}
}

func (sess *session) handlePublish(f Frame) {
	t, err := topic.New(f.Topic)
	if err != nil {
		sess.sendError(f.ReqID, fmt.Sprintf("invalid topic: %v", err))
		return
	}
	msg := message.NewMessageWithHeaders(t, f.Data, f.Headers)
	delivered, dropped := sess.b.Publish(sess.ctx, msg)
	sess.sendFrame(Frame{
		Type:      FrameAck,
		ReqID:     f.ReqID,
		Delivered: delivered,
		Dropped:   dropped,
	})
}

func (sess *session) handleSubscribe(f Frame) {
	t, err := topic.New(f.Topic)
	if err != nil {
		sess.sendError(f.ReqID, fmt.Sprintf("invalid topic: %v", err))
		return
	}
	subCtx, subCancel := context.WithCancel(sess.ctx)
	sub := sess.b.Subscribe(subCtx, t, subscription.WithBufferSize(200))

	sess.subsMu.Lock()
	sess.subs[sub.ID()] = subCancel
	sess.subsMu.Unlock()

	sess.sendFrame(Frame{
		Type:           FrameAck,
		ReqID:          f.ReqID,
		SubscriptionID: sub.ID(),
	})

	go sess.forwardMessages(sub, subCtx)
}

func (sess *session) handleSubscribePattern(f Frame) {
	p, err := topic.NewPattern(f.Pattern)
	if err != nil {
		sess.sendError(f.ReqID, fmt.Sprintf("invalid pattern: %v", err))
		return
	}
	subCtx, subCancel := context.WithCancel(sess.ctx)
	sub := sess.b.SubscribePattern(subCtx, p, subscription.WithBufferSize(200))

	sess.subsMu.Lock()
	sess.subs[sub.ID()] = subCancel
	sess.subsMu.Unlock()

	sess.sendFrame(Frame{
		Type:           FrameAck,
		ReqID:          f.ReqID,
		SubscriptionID: sub.ID(),
	})

	go sess.forwardMessages(sub, subCtx)
}

func (sess *session) handleUnsubscribe(f Frame) {
	sess.subsMu.Lock()
	cancel, ok := sess.subs[f.SubscriptionID]
	if ok {
		delete(sess.subs, f.SubscriptionID)
	}
	sess.subsMu.Unlock()

	if ok {
		cancel()
	}
}

// forwardMessages reads from sub.MessageChannel and writes MESSAGE frames to
// the client until the subscription is closed or the context is done.
func (sess *session) forwardMessages(sub *subscription.Subscription, ctx context.Context) {
	for {
		select {
		case msg, ok := <-sub.MessageChannel():
			if !ok {
				return
			}
			data, err := dataToRawJSON(msg.Data())
			if err != nil {
				log.Printf("transport: marshal message data: %v", err)
				continue
			}
			sess.sendFrame(Frame{
				Type:           FrameMessage,
				SubscriptionID: sub.ID(),
				Topic:          msg.Topic().String(),
				Data:           data,
				Headers:        msg.Headers(),
				MsgID:          msg.ID(),
				PublishedAt:    msg.PublishedAt().UnixNano(),
			})
		case <-ctx.Done():
			return
		}
	}
}

// dataToRawJSON converts any value to a json.RawMessage.
// If the value is already json.RawMessage it is returned as-is to avoid
// double-encoding (e.g. when a message arrived from another transport hop).
func dataToRawJSON(v interface{}) (json.RawMessage, error) {
	if raw, ok := v.(json.RawMessage); ok {
		return raw, nil
	}
	return json.Marshal(v)
}

func (sess *session) sendFrame(f Frame) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if err := writeFrame(sess.bw, f); err != nil {
		log.Printf("transport: server write frame: %v", err)
		return
	}
	if err := sess.bw.Flush(); err != nil {
		log.Printf("transport: server flush: %v", err)
	}
}

func (sess *session) sendError(reqID, msg string) {
	sess.sendFrame(Frame{
		Type:  FrameError,
		ReqID: reqID,
		Error: msg,
	})
}
