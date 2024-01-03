package rabbit

import (
	"fmt"
	"log"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func nowTime() string {
	return time.Now().Format("2006-01-02 15:04:05")
}

func TestChannel_Publish(t *testing.T) {
	var (
		conf = Conf{
			User: "guest",
			Pwd:  "guest",
			Addr: "127.0.0.1",
			Port: "5672",
		}

		exchangeName = "user.register.direct"
		queueName    = "user.register.queue"
		keyName      = "user.register.event"
	)

	if err := Init(conf); err != nil {
		log.Fatalf(" mq init err: %v", err)
	}

	cfg := QueueConfig{
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
	}

	p, err := NewRabbitMQ(exchangeName, "direct", keyName, queueName, &cfg)
	if err != nil {
		log.Fatalf("create channel err: %v", err)
	}

	go func() {
		if err := NewConsumer(queueName, func(body []byte) error {
			fmt.Println("consume msg :" + string(body))
			return nil
		}); err != nil {
			log.Fatalf("consume err: %v", err)
		}
	}()

	go func() {
		for {
			if err := p.Publish([]byte(nowTime())); err != nil {
				log.Fatalf("publish msg err: %v", err)
			}
			time.Sleep(time.Second)
		}
	}()

	time.Sleep(time.Minute)
	t.Log("end")
}

func TestChannel_PublishWithDelay(t *testing.T) {
	var (
		conf = Conf{
			User: "guest",
			Pwd:  "guest",
			Addr: "127.0.0.1",
			Port: "5672",
		}

		exchangeName   = "user.delay.direct"
		queueName      = "user.delay.queue"
		delayQueueName = "user.delay1.queue" // 延迟队列
		keyName        = "user.delay.event"
		delayKeyName   = "user.delay1.event" // 延迟key
	)

	if err := Init(conf); err != nil {
		log.Fatalf(" mq init err: %v", err)

	}
	cfg := QueueConfig{
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
	}

	p, err := NewRabbitMQ(exchangeName, "direct", keyName, queueName, &cfg)
	if err != nil {
		log.Fatalf("create channel err: %v", err)
	}
	p.Publish([]byte(nowTime()))

	wcfg := QueueConfig{
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		Args: amqp.Table{
			"x-dead-letter-exchange":    exchangeName,
			"x-dead-letter-routing-key": keyName,
		},
	}

	pw, err := NewRabbitMQ(exchangeName, "direct", delayKeyName, delayQueueName, &wcfg)
	if err != nil {
		log.Fatalf("create channel err: %v", err)
	}
	p.Publish([]byte(nowTime()))

	go func() {
		if err := NewConsumer(queueName, func(body []byte) error {
			fmt.Println(fmt.Sprintf("consumer msg: %s, ts: %s", string(body), nowTime()))
			return nil
		}); err != nil {
			log.Fatalf("consume err: %v", err)
		}
	}()
	go func() {
		for {
			if err := pw.PublishWithDelay(exchangeName, keyName, []byte(nowTime()), 200*time.Millisecond); err != nil {
				log.Fatalf("publish msg err: %v", err)
			}
			time.Sleep(time.Second)
		}
	}()

	time.Sleep(time.Minute)
	t.Log("end")
}
