package rabbit

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/baowk/dilu-plugin/mq"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Conf struct {
	Addr string
	Port string
	User string
	Pwd  string
}

var (
	defaultConn *Connection
)

type RabbitMQ struct {
	ch         *Channel
	exchange   string
	kind       string
	routingKey string
	queueName  string
	conf       *QueueConfig
}

type QueueConfig struct {
	Durable    bool //持久化
	AutoDelete bool //自动删除
	Exclusive  bool // 排他队列
	Inclusive  bool // 包含交换机
	NoWait     bool // 非阻塞
	Mandatory  bool
	Immediate  bool
	Args       amqp.Table
}

func NewRabbitMQ(exchange, kind, routingKey, queueName string, cfg *QueueConfig) (*RabbitMQ, error) {
	ch, err := defaultConn.Channel()
	if err != nil {
		return nil, fmt.Errorf("new mq channel err: %v", err)
	}
	if err := ch.ExchangeDeclare(exchange, kind, cfg); err != nil {
		return nil, fmt.Errorf("create exchange err: %v", err)
	}
	if err := ch.QueueDeclare(queueName, cfg); err != nil {
		return nil, fmt.Errorf("create queue err: %v", err)
	}
	if err := ch.QueueBind(queueName, routingKey, exchange, cfg); err != nil {
		return nil, fmt.Errorf("bind queue err: %v", err)
	}
	p := &RabbitMQ{exchange: exchange, kind: kind, routingKey: routingKey, queueName: queueName, ch: ch, conf: cfg}
	return p, nil
}

func (p *RabbitMQ) Publish(data []byte) error {
	_, err := p.ch.Channel.PublishWithDeferredConfirmWithContext(context.Background(), p.exchange, p.routingKey, p.conf.Mandatory, p.conf.Immediate,
		amqp.Publishing{ContentType: "text/plain", Body: data})
	return err
}

func (RabbitMQ) String() string {
	return "rabbit"
}

// PublishWithDelay 发布延迟消息.
func (p *RabbitMQ) PublishWithDelay(exchange, key string, body []byte, timer time.Duration) (err error) {
	_, err = p.ch.Channel.PublishWithDeferredConfirmWithContext(context.Background(), exchange, key, p.conf.Mandatory, p.conf.Immediate,
		amqp.Publishing{ContentType: "text/plain", Body: body, Expiration: fmt.Sprintf("%d", timer.Milliseconds())})
	return err
}

func (e *RabbitMQ) Register(queueName string, f mq.ConsumerFunc) {
	deliveries, err := e.ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		panic(fmt.Sprintf("consume err: %v, queue: %s", err, queueName))
	}

	for msg := range deliveries {

		var cmsg mq.Message
		err = json.Unmarshal(msg.Body, &cmsg)
		if err != nil {
			_ = msg.Reject(true)
			continue
		}
		err = f(&cmsg)
		if err != nil {
			_ = msg.Reject(true)
			continue
		}
		_ = msg.Ack(false)
	}
}

// Init 初始化
func Init(c Conf) (err error) {
	if c.Addr == "" {
		return nil
	}
	defaultConn, err = Dial(fmt.Sprintf("amqp://%s:%s@%s:%s/",
		c.User,
		url.QueryEscape(c.Pwd),
		c.Addr,
		c.Port))
	if err != nil {
		return fmt.Errorf("new mq conn err: %v", err)
	}
	return
}

// NewChannel 获取channel.
func NewChannel() (*Channel, error) {
	channel, err := defaultConn.Channel()
	if err != nil {
		return nil, fmt.Errorf("new mq channel err: %v", err)
	}
	return channel, nil
}

// ExchangeDeclare 创建交换机.
func (ch *Channel) ExchangeDeclare(name string, kind string, cfg *QueueConfig) (err error) {
	return ch.Channel.ExchangeDeclare(name, kind, cfg.Durable, cfg.AutoDelete, cfg.Inclusive, cfg.NoWait, cfg.Args)
}

// QueueDeclare 创建队列.
func (ch *Channel) QueueDeclare(name string, cfg *QueueConfig) (err error) {
	_, err = ch.Channel.QueueDeclare(name, cfg.Durable, cfg.AutoDelete, cfg.Exclusive, cfg.NoWait, cfg.Args)
	return
}

// QueueDeclareWithDelay 创建延迟队列.
func (ch *Channel) QueueDeclareWithDelay(name, exchange, key string, cfg *QueueConfig) (err error) {
	_, err = ch.Channel.QueueDeclare(name, cfg.Durable, cfg.AutoDelete, cfg.Exclusive, cfg.NoWait, amqp.Table{
		"x-dead-letter-exchange":    exchange,
		"x-dead-letter-routing-key": key,
	})
	return
}

// QueueBind 绑定队列.
func (ch *Channel) QueueBind(name, key, exchange string, conf *QueueConfig) (err error) {
	return ch.Channel.QueueBind(name, key, exchange, conf.NoWait, conf.Args)
}

// NewConsumer 实例化一个消费者, 会单独用一个channel.
func NewConsumer(queue string, handler func([]byte) error) error {
	ch, err := defaultConn.Channel()
	if err != nil {
		return fmt.Errorf("new mq channel err: %v", err)
	}

	deliveries, err := ch.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume err: %v, queue: %s", err, queue)
	}

	for msg := range deliveries {
		err = handler(msg.Body)
		if err != nil {
			_ = msg.Reject(true)
			continue
		}
		_ = msg.Ack(false)
	}

	return nil
}
