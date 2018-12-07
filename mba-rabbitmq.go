package mbarabbitmqdriver

import (
	"encoding/json"

	"mba"

	"github.com/streadway/amqp"
)

// Name of the mba rabbitmq driver
const Name string = "rabbitmq"

// RmqDriver represents the rabbitMQ driver context for the mba package.
type RmqDriver struct {
	conn *amqp.Connection
}

type RmqPublisher struct {
	channel *amqp.Channel
	queue   *amqp.Queue
}

type RmqSubscriber struct {
	channel    *amqp.Channel
	queue      *amqp.Queue
	do         func(msg interface{}) error
	deliveries <-chan amqp.Delivery
}

// New returns an rabbitMQ driver instance.
func New(url string) (*RmqDriver, error) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")

	if err == nil {
		return &RmqDriver{conn: conn}, nil
	}

	//defer conn.Close()

	return nil, err
}

// NewPublisher returns a new rabbitMQ Publisher
func (r *RmqDriver) NewPublisher(topic string) (mba.Publisher, error) {
	ch, err := r.conn.Channel()
	//failOnError(err, "Failed to open a channel")
	//defer ch.Close()

	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		topic, // queue name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	if err != nil {
		return nil, err
	}

	return &RmqPublisher{channel: ch, queue: &q}, nil
}

// Send sends a message to the messages queue.
func (p *RmqPublisher) Send(msg interface{}) error {

	json, err := json.Marshal(msg)

	if err != nil {
		return err
	}

	body := json
	err = p.channel.Publish(
		"",           // exchange
		p.queue.Name, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(body),
		})

	return err
}

func (p *RmqPublisher) Close() {
	//TODO
}

// NewSubscriber returns a new rabbitMQ Publisher
func (r *RmqDriver) NewSubscriber() (mba.Subscriber, error) {
	ch, err := r.conn.Channel()

	if err != nil {
		return nil, err
	}

	//	defer ch.Close()

	return &RmqSubscriber{channel: ch}, nil
}

// SetTopic sets the function to run when a message with the dedicated topic is received on the subsbriber.
func (r RmqDriver) SetTopic(mbs mba.Subscriber, topic string, f func(msg interface{}) error) error {

	mms := mbs.(*RmqSubscriber)

	q, err := mms.channel.QueueDeclare(
		topic, // queue name
		false, // durable
		false, // delete when usused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	if err != nil {
		return err
	}

	mms.queue = &q
	mms.do = f

	deliveries, err := mms.channel.Consume(
		mms.queue.Name, // queue
		"",             // consumer
		true,           // auto-ack
		false,          // exclusive
		false,          // no-local
		false,          // no-wait
		nil,            // args
	)

	if err != nil {
		return err
	}

	mms.deliveries = deliveries

	return nil
}

// Receive blocks and runs the configured function when a message arrives.
// If the returned value ok is set to false, no more messages can be received with the current configuration.
func (s *RmqSubscriber) Receive() (bool, error) {

	msg := <-s.deliveries

	return true, s.do(msg.Body)
}
