package hqclient

import (
	"errors"
	"log"
	"net/url"
	"sync"

	"github.com/gorilla/websocket"
)

type topic = string

// represents connection to broker and ques for topics
type HqConnection struct {
	WsConn      *websocket.Conn  // connection to the mb
	EventQueues map[topic][]*Que // Ques for a given topic BAD NAME subscibers is better
	GlobalQue   *Que
	mu          sync.RWMutex
}

// Que struct representing the Que in the actuall message broker
// allows for lisineing to incoming event comming trough ws connection
// and groups them toghether to correct que
type Que struct {
	EventStream EventStream
}

// Event stream is just a wrapper arround chan for nicer checking if chan is closed
// plus makes refactors easier
type EventStream struct {
	msg    chan Message
	closed bool
}

const EventStreamBuffSize = 1000

var Subscribe = byte(1)
var Publish = byte(2)
var Pop = byte(3)
var Unsubscribe = byte(4)
var Ping = byte(5) //TODO
var Pong = byte(6) //TODO
var Ack = byte(7)
var Nack = byte(8)
var Merror = byte(9)
var AddTopic = byte(10)
var GlobalSub = byte(11)
var DeleteTopic = byte(12)

// retunrs a hq connection, and starts go routine that lisens to incoming WS messages grouping them to correct queue
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
	hq := &HqConnection{WsConn: conn, Queue: make(map[topic][]*Que, 0)}

	// starting go rutine to lisen to incoming messages from ws conn
	go hq.lisenToMessages()
	return hq, nil
}

func newQue(topic string) *Que {
	return &Que{
		EventStream: newEventStream(EventStreamBuffSize),
		Topic:       topic,
		// Messages:    make([]Message, 0),
	}
}

func newEventStream(buffSize int) EventStream {
	return EventStream{
		msg:    make(chan Message, buffSize),
		closed: false,
	}
}

// TODO better error handling, with global sub
// drops messages when channel is full, default size is 1000
func (e *EventStream) notifyEvent(event Message) error {
	if e.closed {
		return errors.New("channel is closed")
	}
	select {
	case e.msg <- event:
		return nil
	default:
		return errors.New("channel is full, dopoing the message")
	}
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
// func (q *Que) quePush() {

// }

func (hq *HqConnection) closeAllQues() {
	hq.mu.Lock()
	defer hq.mu.Unlock()
	for _, queue := range hq.Queue {
		for _, que := range queue {
			que.closeQue()
		}
	}
	hq.GlobalQue.closeQue()
}

// TODO Maybe change this latter
// func (hq *HqConnection) globalSubLisen() {
// }

// lisens to incoming messages trough ws conn, will stop if conneciton errors out
func (hq *HqConnection) lisenToMessages() {
	defer hq.closeAllQues()
	// go hq.globalSubLisen()

	for {
		_, payload, err := hq.WsConn.ReadMessage()
		if err != nil {
			log.Println("Error reading WebSocket message:", err)
			break
		}

		message, err := ParseMessage(payload)
		if err != nil {
			log.Println("Error parsing message:", err)
			continue
		}

		topic, ok := message.Headers["topic"]
		// sending message to global channel
		hq.GlobalQue.EventStream.notifyEvent(*message)
		// skipping if topic does not exits in the message
		if !ok {
			log.Println("message did not contain topic")
			continue
		}

		queue, exists := hq.Queue[topic]
		if !exists {
			log.Printf("No queue for topic: %s", topic)
			continue
		}
		// sends event to all ques for a given topic
		for _, que := range queue {
			err := que.EventStream.notifyEvent(*message)
			if err != nil {
				log.Println(err)
			}
		}
	}
}

// wheanerv someting that topic == topic send back to what you return
func (hq *HqConnection) Subscribe(topic string) (*Que, error) {
	header := map[string]string{
		"topic": topic,
	}
	msg := NewMessage(Subscribe, header, "").SerializeMessage()
	if err := hq.WsConn.WriteMessage(websocket.BinaryMessage, msg); err != nil {
		return nil, err
	}
	return newQue(topic), nil
}

func (hq *HqConnection) Publish(msg *Message) error {
	if msg.Action != Publish {
		return errors.New("message action type is not publish change that")
	}
	if _, ok := msg.Headers["topic"]; !ok {
		return errors.New("message missing topic for publish action")
	}
	return hq.WsConn.WriteMessage(websocket.BinaryMessage, msg.SerializeMessage())
}

// TODO better errors,
func (hq *HqConnection) AddTopic(topic string) error {
	header := map[string]string{
		"topic": topic,
	}
	msg := NewMessage(AddTopic, header, "").SerializeMessage()
	return hq.WsConn.WriteMessage(websocket.BinaryMessage, msg)
}

// add deleting ques
func (hq *HqConnection) DeleteTopic(topic string) error {
	hq.mu.Lock()
	defer hq.mu.Unlock()

	header := map[string]string{
		"topic": topic,
	}
	msg := NewMessage(DeleteTopic, header, "").SerializeMessage()
	for _, que := range hq.Queue[topic] {
		que.closeQue()
	}
	delete(hq.Queue, topic)

	return hq.WsConn.WriteMessage(websocket.BinaryMessage, msg)
}

// TODO better errors
func (hq *HqConnection) PopMessage(topic string) error {
	header := map[string]string{
		"topic": topic,
	}
	msg := NewMessage(Pop, header, "").SerializeMessage()
	return hq.WsConn.WriteMessage(websocket.BinaryMessage, msg)
}

// TODO think bout global sub
// returns a global subsciber for ALL incoming messages, error messages too
func (hq *HqConnection) GlobalSubscibtion(msg *Message) (*Que, error) {
	if msg.Action != GlobalSub {
		return nil, errors.New("message action type is not global")
	}
	err := hq.WsConn.WriteMessage(websocket.BinaryMessage, msg.SerializeMessage())
	if err != nil {
		return nil, err
	}
	return hq.GlobalQue, err
}
