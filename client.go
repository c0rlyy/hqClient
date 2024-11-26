package hqclient

import (
	"errors"
	"log"
	"net/url"

	"github.com/gorilla/websocket"
)

type HqConnection struct {
	WsConn *websocket.Conn
}

type Que struct {
	Messages chan Message
	Topic    string
}

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

// wheanerv someting that topic == topic send back to what you return
func (hq *HqConnection) Subscribe(topic string) (*Que, error) {
	t := make(map[string]string)
	t["topic"] = topic
	// creating dummy message coz im a moron and dont know how else to do this to get lenght
	// i guess i could just do smth like lenght of headers to string or to binarty and just add to that 3 bytes
	tempMsg := NewMessage(0, Subscribe, t, "")
	serialized := tempMsg.SerializeMessage()
	length := uint16(len(serialized))

	msg := NewMessage(length, Subscribe, t, "").SerializeMessage()
	err := hq.WsConn.WriteMessage(websocket.BinaryMessage, msg)
	if err != nil {
		return nil, err
	}
	que := &Que{Topic: topic, Messages: make(chan Message, 100)}
	go func(q *Que) {
		defer close(q.Messages)
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
				que.Messages <- *message
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

func (hq *HqConnection) AddTopic(msg *Message) error {
	if msg.Action != AddTopic {
		return errors.New("message action type is not publish change that")
	}
	err := hq.WsConn.WriteMessage(websocket.BinaryMessage, msg.SerializeMessage())
	return err
}

func (hq *HqConnection) PopMessage(msg *Message) error {
	if msg.Action != Pop {
		return errors.New("message action type is not publish change that")
	}
	err := hq.WsConn.WriteMessage(websocket.BinaryMessage, msg.SerializeMessage())
	return err
}

func (hq *HqConnection) GlobalSubscibtion(msg *Message) (*Que, error) {
	if msg.Action != GlobalSub {
		return nil, errors.New("message action type is not publish change that")
	}
	err := hq.WsConn.WriteMessage(websocket.BinaryMessage, msg.SerializeMessage())
	que := &Que{
		Messages: make(chan Message, 100),
		Topic:    "",
	}
	go func(q *Que) {
		defer close(q.Messages)
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
				que.Messages <- *message
			}
		}
	}(que)
	return que, err
}
