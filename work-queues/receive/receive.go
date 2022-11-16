package main

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s, %s\n", msg, err)
	}
}

func receive() {
	conn, err := amqp.Dial("amqp://admin:admin@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,         // durable, 持久化
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// 设置公平派遣
	ch.Qos(
		1,     // prefetch count = 1, 在worker处理并确认一条信息之前，不要向它发送新的信息。应该把它派发给不忙的worker
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack, 需要手动消息确认
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf("Recieved a message %s\n", d.Body)
			// dotCount := bytes.Count(d.Body, []byte("."))
			// t := time.Duration(dotCount)
			// time.Sleep(t * time.Second)
			// log.Printf("Done")
			d.Ack(false) // 消息确认
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func main() {
	receive()
}
