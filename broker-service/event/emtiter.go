package event

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

type Emitter struct {
	connection *amqp.Connection
}

func (e Emitter) setup() error {
	chanel, err := e.connection.Channel()
	if err != nil {
		return err
	}

	defer chanel.Close()

	return declareExchange(chanel)
}

func (e *Emitter) Push(event string, severity string) error {
	chanel, err := e.connection.Channel()
	if err != nil {
		return err
	}

	defer chanel.Close()

	log.Println("Pushing to chanel")

	err = chanel.Publish(
		"logs_topic",
		severity,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(event),
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func NewEventEmitter(conn *amqp.Connection) (Emitter, error) {
	emitter := Emitter{
		connection: conn,
	}

	err := emitter.setup()
	if err != nil {
		return Emitter{}, err
	}

	return emitter, nil
}
