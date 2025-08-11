package kqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	model "WBTests/internal/Models"
	"WBTests/internal/cache"
	"WBTests/internal/store"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	Brokers []string
	Topic   string
	GroupID string
}

type Consumer struct {
	r     *kafka.Reader
	store store.Store
	cache *cache.Cache
}

func NewConsumer(cfg Config, s store.Store, c *cache.Cache) (*Consumer, error) {
	if len(cfg.Brokers) == 0 || cfg.Brokers[0] == "" {
		return nil, fmt.Errorf("kafka: empty brokers")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, fmt.Errorf("kafka: empty topic")
	}
	if strings.TrimSpace(cfg.GroupID) == "" {
		return nil, fmt.Errorf("kafka: empty group id")
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		GroupID:  cfg.GroupID,
		Topic:    cfg.Topic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	return &Consumer{r: r, store: s, cache: c}, nil
}

func validate(o *model.Order) error {
	if o.OrderUid == "" {
		return fmt.Errorf("empty order_uid")
	}
	if o.CustomerId == "" {
		return fmt.Errorf("empty customer_id")
	}
	if len(o.Items) == 0 {
		return fmt.Errorf("empty items")
	}
	return nil
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.r.Close()

	for {
		msg, err := c.r.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			log.Printf("[kafka] fetch error: %v", err)
			time.Sleep(300 * time.Millisecond)
			continue
		}

		var o model.Order
		if err := json.Unmarshal(msg.Value, &o); err != nil {
			log.Printf("[kafka] bad json (offset=%d): %v", msg.Offset, err)
			if err := c.r.CommitMessages(ctx, msg); err != nil {
				log.Printf("[kafka] commit after bad json error: %v", err)
			}
			continue
		}

		if err := validate(&o); err != nil {
			log.Printf("[kafka] validation failed (offset=%d): %v", msg.Offset, err)
			if err := c.r.CommitMessages(ctx, msg); err != nil {
				log.Printf("[kafka] commit after bad validate error: %v", err)
			}
			continue
		}

		if err := c.store.UpsertOrder(ctx, o); err != nil {
			log.Printf("[kafka] db upsert error (will retry): %v (offset=%d)", err, msg.Offset)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		c.cache.Set(o.OrderUid, o)

		// подтверждаем смещение
		if err := c.r.CommitMessages(ctx, msg); err != nil {
			log.Printf("[kafka] commit error: %v", err)
		}
	}
}
