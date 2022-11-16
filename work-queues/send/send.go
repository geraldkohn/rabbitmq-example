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
		log.Panicf("%s, %s\n", msg, err)
	}
}

// func bodyFrom(args []string) string {
// 	var s string
// 	if len(args) < 2 || os.Args[1] == "" {
// 		s = "hello"
// 	} else {
// 		s = strings.Join(args[1:], "")
// 	}
// 	return s
// }

func send(i int) {
	conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,         // durable, 持久化
		false,        // delete when used, 未使用时删除
		false,        // exclusive, 这个队列只能由这个连接处理
		false,        // no wait
		nil,          // args
	)
	failOnError(err, "Failed to declare a queue")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := fmt.Sprintf("Hello world! %d", i)
	// body := bodyFrom(os.Args)
	// 发布消息
	err = ch.PublishWithContext(ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		})
	failOnError(err, "Failed to send a message")
	log.Printf("[X] Sent %s\n", body)
}

func main() {
	for i := 0; i < 10000; i++ {
		time.Sleep(10 * time.Millisecond)
		send(i)
	}
}
