package main

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func send(routingKey string) {
	conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs_topic", // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// body := bodyFrom(os.Args)
	body := fmt.Sprintf("Topic: routing key: %s", routingKey)
	err = ch.PublishWithContext(ctx,
		"logs_topic", // exchange
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")

	log.Printf(" [x] Sent %s", body)
}

func main() {
	for i := 0; i < 1000; i++ {
		time.Sleep(200 * time.Millisecond)
		send("quick.orange.rabbit")
		time.Sleep(200 * time.Millisecond)
		send("lazy.orange.elephant")
		time.Sleep(200 * time.Millisecond)
		send("lazy.brown.fox")
		time.Sleep(200 * time.Millisecond)
		send("lazy.pink.rabbit")
		time.Sleep(200 * time.Millisecond)
		send("quick.brown.fox")
	}
}
