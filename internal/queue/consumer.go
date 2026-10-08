package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

var ErrDeadLetter = errors.New("dead letter rabbitmq message")

type Handler func(ctx context.Context, message EventCreatedMessage) error

type RabbitMQConsumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   string
}

func NewRabbitMQConsumer(cfg Config) (*RabbitMQConsumer, error) {
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("connect rabbitmq: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open rabbitmq channel: %w", err)
	}

	consumer := &RabbitMQConsumer{
		conn:    conn,
		channel: channel,
		queue:   cfg.DeliveryQueue,
	}

	if err := declareDeliveryTopology(channel, cfg); err != nil {
		consumer.Close()
		return nil, err
	}

	if err := channel.Qos(1, 0, false); err != nil {
		consumer.Close()
		return nil, fmt.Errorf("configure rabbitmq qos: %w", err)
	}

	return consumer, nil
}

func (c *RabbitMQConsumer) Consume(ctx context.Context, handler Handler) error {
	deliveries, err := c.channel.Consume(
		c.queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("consume rabbitmq deliveries: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("rabbitmq delivery channel closed")
			}

			if err := c.handleDelivery(ctx, delivery, handler); err != nil {
				return err
			}
		}
	}
}

func (c *RabbitMQConsumer) Close() error {
	var err error

	if c.channel != nil {
		if closeErr := c.channel.Close(); closeErr != nil {
			err = closeErr
		}
	}
	if c.conn != nil {
		if closeErr := c.conn.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}

	return err
}

func (c *RabbitMQConsumer) handleDelivery(ctx context.Context, delivery amqp.Delivery, handler Handler) error {
	var message EventCreatedMessage
	if err := json.Unmarshal(delivery.Body, &message); err != nil {
		return ack(delivery)
	}

	if strings.TrimSpace(message.EventID) == "" {
		return ack(delivery)
	}

	if err := handler(ctx, message); err != nil {
		if errors.Is(err, ErrDeadLetter) {
			return nackDeadLetter(delivery)
		}
		return nackRequeue(delivery)
	}

	return ack(delivery)
}

func ack(delivery amqp.Delivery) error {
	if err := delivery.Ack(false); err != nil {
		return fmt.Errorf("ack rabbitmq message: %w", err)
	}
	return nil
}

func nackRequeue(delivery amqp.Delivery) error {
	if err := delivery.Nack(false, true); err != nil {
		return fmt.Errorf("nack rabbitmq message: %w", err)
	}
	return nil
}

func nackDeadLetter(delivery amqp.Delivery) error {
	if err := delivery.Nack(false, false); err != nil {
		return fmt.Errorf("dead-letter rabbitmq message: %w", err)
	}
	return nil
}

func declareDeliveryTopology(channel *amqp.Channel, cfg Config) error {
	if err := channel.ExchangeDeclare(
		cfg.Exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare rabbitmq exchange: %w", err)
	}

	if err := channel.ExchangeDeclare(
		cfg.DeadLetterExchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare rabbitmq dead letter exchange: %w", err)
	}

	if _, err := channel.QueueDeclare(
		cfg.DeadLetterQueue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare rabbitmq dead letter queue: %w", err)
	}

	if err := channel.QueueBind(
		cfg.DeadLetterQueue,
		cfg.DeliveryQueue,
		cfg.DeadLetterExchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind rabbitmq dead letter queue: %w", err)
	}

	if _, err := channel.QueueDeclare(
		cfg.DeliveryQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    cfg.DeadLetterExchange,
			"x-dead-letter-routing-key": cfg.DeliveryQueue,
		},
	); err != nil {
		return fmt.Errorf("declare rabbitmq delivery queue: %w", err)
	}

	if err := channel.QueueBind(
		cfg.DeliveryQueue,
		cfg.DeliveryQueue,
		cfg.Exchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind rabbitmq delivery queue: %w", err)
	}

	return nil
}
