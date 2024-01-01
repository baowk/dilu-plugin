package redis

import (
	"context"

	"github.com/baowk/dilu-plugin/mq"
	"github.com/redis/go-redis/v9"
)

// NewRedis redis模式
func NewRedis(client *redis.Client) (*Redis, error) {
	//var err error
	r := &Redis{client: client}
	// r.producer, err = r.newProducer(producerOptions)
	// if err != nil {
	// 	return nil, err
	// }
	// r.consumer, err = r.newConsumer(consumerOptions)
	// if err != nil {
	// 	return nil, err
	// }
	return r, nil
}

// Redis cache implement
type Redis struct {
	client *redis.Client
	// consumer *redisqueue.Consumer
	// producer *redisqueue.Producer
}

func (Redis) String() string {
	return "redis"
}

// func (r *Redis) newConsumer(options *redisqueue.ConsumerOptions) (*redisqueue.Consumer, error) {
// 	if options == nil {
// 		options = &redisqueue.ConsumerOptions{}
// 	}
// 	return redisqueue.NewConsumerWithOptions(options)
// }

// func (r *Redis) newProducer(options *redisqueue.ProducerOptions) (*redisqueue.Producer, error) {
// 	if options == nil {
// 		options = &redisqueue.ProducerOptions{}
// 	}
// 	return redisqueue.NewProducerWithOptions(options)
// }

func (r *Redis) Publish(message mq.IMessager) error {
	// pubsub := r.client.Publish(context.Background(), message.GetStream(), message)
	// _, err := pubsub.Receive(context.Background())
	// if err != nil {
	// 	panic(err)
	// }

	// ch := pubsub.Channel()
	// for msg := range ch {
	// 	fmt.Println(msg.Channel, msg.Payload)
	// }
	// return nil
	return nil
}

func (r *Redis) Register(name string, f mq.ConsumerFunc) {
	r.client.Subscribe(context.Background(), name)
}

func (r *Redis) Run() {
	//r.Run()
}

func (r *Redis) Shutdown() {
	r.client.Subscribe(context.Background(), "test").Close()
}
