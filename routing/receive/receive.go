package main

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func receive(routingKey string, ctx context.Context) {
	conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,        // queue name
		routingKey,    // routing key
		"logs_direct", // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	go func(ctx context.Context) {
		for d := range msgs {
			select {
			case <-ctx.Done():
				return
			default:
				log.Printf(" [x] %s", d.Body)
			}
		}
	}(ctx)

	select {
	case <-ctx.Done():
		return
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	go receive("white", ctx)
	go receive("black", ctx)
	defer cancel()

	var forever chan struct{}
	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}
