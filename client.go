package hqclient

import (
	"errors"
	"log"
	"net/url"
	"sync"

	"github.com/gorilla/websocket"
)

type HqConnection struct {
	WsConn *websocket.Conn
}

// Que struct representing the Que in the actuall message broker
// allows for lisineing to incoming event comming trough ws connection
// and groups them toghether to correct que
type Que struct {
	EventStream EventStream
	Topic       string
	Messages    []Message // not sure if i want this
	mu          *sync.RWMutex
}

// Event stream is just a wrapper arround chan for nicer checking if chan is closed
// plus makes refactors easier
type EventStream struct {
	msg    chan Message
	closed bool
}

const EventStreamBuffSize = 100

var Subscribe = byte(1)
var Publish = byte(2)
var Pop = byte(3)
var Unsubscribe = byte(4)
var Ping = byte(5)
var Pong = byte(6)
var Ack = byte(7)
var Nack = byte(8)
var Merror = byte(9)
var AddTopic = byte(10)
var GlobalSub = byte(11)
var DeleteTopic = byte(12)

func NewHqConnection(brokerUrl string) (*HqConnection, error) {
	u, err := url.Parse(brokerUrl)
	if err != nil {
		return nil, err
	}
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return nil, err
	}
	log.Println("Connected to WebSocket server:", u.String())
	return &HqConnection{WsConn: conn}, nil
}

func newQue(topic string) *Que {
	return &Que{
		EventStream: newEventStream(EventStreamBuffSize),
		Topic:       topic,
		Messages:    make([]Message, 0),
	}
}

func newEventStream(buffSize int) EventStream {
	return EventStream{
		msg:    make(chan Message, buffSize),
		closed: false,
	}
}

func (e *EventStream) notifyEvent(event Message) error {
	if e.closed {
		return errors.New("channel is closed")
	}
	e.msg <- event
	return nil
}

// return channel that lisins for incoming messages
// this fucntion only exits coz im lazy and i didnt feel like changing
// code in so many places
func (e *EventStream) Msg() (chan Message, error) {
	if e.closed {
		return nil, errors.New("channel is closed")
	}
	return e.msg, nil
}

func (q *Que) closeQue() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.EventStream.closed {
		close(q.EventStream.msg)
		q.EventStream.closed = true
	}
}

// TODO
func (q *Que) quePush() {
}

// wheanerv someting that topic == topic send back to what you return
func (hq *HqConnection) Subscribe(topic string) (*Que, error) {
	t := make(map[string]string)
	t["topic"] = topic
	msg := NewMessage(Subscribe, t, "").SerializeMessage()
	err := hq.WsConn.WriteMessage(websocket.BinaryMessage, msg)
	if err != nil {
		return nil, err
	}
	que := newQue(topic)
	// TODO think whather this is the best place for this gorutine
	go func(q *Que) {
		defer q.closeQue()
		for {
			_, payload, err := hq.WsConn.ReadMessage()
			if err != nil {
				log.Println("Error reading WebSocket message:", err)
				break
			}

			message, err := ParseMessage(payload)
			if err != nil {
				log.Println("Error parsing message:", err)
				break
			}

			if q.Topic == message.Headers["topic"] {
				if err := q.EventStream.notifyEvent(*message); err != nil {
					log.Printf("error: sending to closed que %v", q.Topic)
					break
				}
			}

		}
	}(que)
	return que, nil
}

func (hq *HqConnection) Publish(msg *Message) error {
	if msg.Action != Publish {
		return errors.New("message action type is not publish change that")
	}
	err := hq.WsConn.WriteMessage(websocket.BinaryMessage, msg.SerializeMessage())
	return err
}

// TODO just topic here
func (hq *HqConnection) AddTopic(topic string) error {
	t := make(map[string]string)
	t["topic"] = topic
	msg := NewMessage(AddTopic, t, "").SerializeMessage()
	err := hq.WsConn.WriteMessage(websocket.BinaryMessage, msg)
	return err
}

// TODO just topic should be here
func (hq *HqConnection) PopMessage(topic string) error {
	t := make(map[string]string)
	t["topic"] = topic
	msg := NewMessage(AddTopic, t, "").SerializeMessage()
	err := hq.WsConn.WriteMessage(websocket.BinaryMessage, msg)
	return err
}

func (hq *HqConnection) GlobalSubscibtion(msg *Message) (*Que, error) {
	if msg.Action != GlobalSub {
		return nil, errors.New("message action type is not global")
	}
	err := hq.WsConn.WriteMessage(websocket.BinaryMessage, msg.SerializeMessage())
	if err != nil {
		return nil, err
	}
	que := newQue("")
	go func(q *Que) {
		defer que.closeQue()
		for {
			_, payload, err := hq.WsConn.ReadMessage()
			if err != nil {
				log.Println("Error reading WebSocket message:", err)
				break
			}
			message, err := ParseMessage(payload)
			if err != nil {
				log.Println("Error parsing message:", err)
				break
			}
			if message.Action == GlobalSub {
				if err := que.EventStream.notifyEvent(*message); err != nil {
					log.Printf("channel was closed for %v topic", que.Topic)
					break
				}
			}
		}
	}(que)
	return que, err
}
