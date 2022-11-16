package main

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s, %s\n", msg, err)
	}
}

func send() {
	conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		false, // durable
		false, // delete when used
		false, // exclusive
		false, // no wait
		nil, // args
	)
	failOnError(err, "Failed to declare a queue")

	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()

	body := "Hello world!"
	err = ch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body: []byte(body),
	})
	failOnError(err, "Failed to send a message")
	log.Printf("[X] Sent %s\n", body)
}

func main() {
	for i := 0; i < 100; i++ {
		time.Sleep(1 * time.Second)
		send()
	}
}