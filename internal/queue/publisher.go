package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Config struct {
	URL                string
	Exchange           string
	DeliveryQueue      string
	DeadLetterExchange string
	DeadLetterQueue    string
}

type EventCreatedMessage struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	QueuedAt      time.Time `json:"queued_at"`
}

type RabbitMQPublisher struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	exchange     string
	routingKey   string
	deliveryMode uint8
}

func NewRabbitMQPublisher(cfg Config) (*RabbitMQPublisher, error) {
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("connect rabbitmq: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open rabbitmq channel: %w", err)
	}

	publisher := &RabbitMQPublisher{
		conn:         conn,
		channel:      channel,
		exchange:     cfg.Exchange,
		routingKey:   cfg.DeliveryQueue,
		deliveryMode: amqp.Persistent,
	}

	if err := declareDeliveryTopology(channel, cfg); err != nil {
		publisher.Close()
		return nil, err
	}

	return publisher, nil
}

func (p *RabbitMQPublisher) PublishEventCreated(ctx context.Context, message EventCreatedMessage) error {
	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal event created message: %w", err)
	}

	if err := p.channel.PublishWithContext(ctx,
		p.exchange,
		p.routingKey,
		false,
		false,
		amqp.Publishing{
			CorrelationId: message.CorrelationID,
			ContentType:   "application/json",
			DeliveryMode:  p.deliveryMode,
			Timestamp:     time.Now().UTC(),
			Type:          "event.created",
			Body:          body,
		},
	); err != nil {
		return fmt.Errorf("publish event created message: %w", err)
	}

	return nil
}

func (p *RabbitMQPublisher) Close() error {
	var err error

	if p.channel != nil {
		if closeErr := p.channel.Close(); closeErr != nil {
			err = closeErr
		}
	}
	if p.conn != nil {
		if closeErr := p.conn.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}

	return err
}
