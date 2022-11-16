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

func send(i int) {
	conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs",   // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// body := bodyFrom(os.Args)
	body := fmt.Sprintf("send %d", i)
	err = ch.PublishWithContext(
		ctx,
		"logs", // exchange
		"",     // routing key, messages are routed to the queue with the name specified by routing_key parameter, if it exists.
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")

	log.Printf(" [x] Sent %s", body)
}

// func bodyFrom(args []string) string {
// 	var s string
// 	if (len(args) < 2) || os.Args[1] == "" {
// 		s = "hello"
// 	} else {
// 		s = strings.Join(args[1:], " ")
// 	}
// 	return s
// }

func main() {
	for i := 0; i < 100000; i++ {
		time.Sleep(10 * time.Millisecond)
		send(i)
	}
}
