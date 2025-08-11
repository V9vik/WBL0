package store

import (
	model "WBTests/internal/Models"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

const upsertOrderSQL = `
INSERT INTO orders (
  order_uid, track_number, entry,
  delivery, payment, items,
  locale, internal_signature, customer_id,
  delivery_service, shardkey, sm_id,
  date_created, oof_shard
)
VALUES (
  $1, $2, $3,
  $4::jsonb, $5::jsonb, $6::jsonb,
  $7, $8, $9,
  $10, $11, $12,
  $13, $14
)
ON CONFLICT (order_uid) DO UPDATE
SET
  track_number       = EXCLUDED.track_number,
  entry              = EXCLUDED.entry,
  delivery           = EXCLUDED.delivery,
  payment            = EXCLUDED.payment,
  items              = EXCLUDED.items,
  locale             = EXCLUDED.locale,
  internal_signature = EXCLUDED.internal_signature,
  customer_id        = EXCLUDED.customer_id,
  delivery_service   = EXCLUDED.delivery_service,
  shardkey           = EXCLUDED.shardkey,
  sm_id              = EXCLUDED.sm_id,
  date_created       = EXCLUDED.date_created,
  oof_shard          = EXCLUDED.oof_shard,
  updated_at         = now(),
  version            = orders.version + 1;

`

type PGStore struct{ pool *pgxpool.Pool }

func NewPGStore(ctx context.Context, dsn string) (*PGStore, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	return &PGStore{
		pool: pool,
	}, nil
}
func (p *PGStore) Close() {
	p.pool.Close()
}

func (p *PGStore) UpsertOrder(ctx context.Context, o model.Order) error {
	deliveryJSON, err := json.Marshal(o.Delivery)
	if err != nil {
		return fmt.Errorf("upsert order: %w", err)
	}
	paymentJSON, err := json.Marshal(o.Payment)
	if err != nil {
		return fmt.Errorf("upsert order: %w", err)
	}
	itemsJSON, err := json.Marshal(o.Items)
	if err != nil {
		return fmt.Errorf("upsert order: %w", err)
	}

	_, err = p.pool.Exec(ctx, upsertOrderSQL,
		o.OrderUid,
		o.TrackNumber,
		o.Entry,
		deliveryJSON,
		paymentJSON,
		itemsJSON,
		o.Locale,
		o.InternalSignature,
		o.CustomerId,
		o.DeliveryService,
		o.ShardKey,
		o.SmId,
		o.DateCreated,
		o.OofShard)
	if err != nil {
		return fmt.Errorf("upsert order: %w", err)
	}
	return nil
}

func (p *PGStore) GetOrder(ctx context.Context, id string) (model.Order, error) {

	const getSQL = `
	  SELECT
		order_uid, track_number, entry,
		delivery, payment, items,
		locale, internal_signature, customer_id,
		delivery_service, shardkey, sm_id,
		date_created, oof_shard,
		created_at, updated_at, version
	  FROM orders
	  WHERE order_uid = $1
	  `
	var (
		orderUID, trackNumber, entry          string
		locale, internalSignature, customerID string
		deliveryService, shardKey, oofShard   string
		deliveryJSON, paymentJSON, itemsJSON  []byte
		smID                                  int32
		dateCreated, createdAt, updatedAt     time.Time
		version                               int
	)
	err := p.pool.QueryRow(ctx, getSQL, id).Scan(
		&orderUID, &trackNumber, &entry,
		&deliveryJSON, &paymentJSON, &itemsJSON,
		&locale, &internalSignature, &customerID,
		&deliveryService, &shardKey, &smID,
		&dateCreated, &oofShard,
		&createdAt, &updatedAt, &version)

	if err != nil {
		return model.Order{}, fmt.Errorf("get order: %w", err)
	}

	var o model.Order
	o.OrderUid = orderUID
	o.TrackNumber = trackNumber
	o.Entry = entry
	o.Locale = locale
	o.InternalSignature = internalSignature
	o.CustomerId = customerID
	o.DeliveryService = deliveryService
	o.ShardKey = shardKey
	o.SmId = int(smID)
	o.DateCreated = dateCreated
	o.OofShard = oofShard
	o.CreatedAt = createdAt
	o.UpdatedAt = updatedAt
	o.Version = version

	if err := json.Unmarshal(deliveryJSON, &o.Delivery); err != nil {
		return model.Order{}, fmt.Errorf("unmarshal delivery: %w", err)
	}
	if err := json.Unmarshal(paymentJSON, &o.Payment); err != nil {
		return model.Order{}, fmt.Errorf("unmarshal payment: %w", err)
	}
	if err := json.Unmarshal(itemsJSON, &o.Items); err != nil {
		return model.Order{}, fmt.Errorf("unmarshal items: %w", err)
	}

	return o, nil
}

func (p *PGStore) ListRecentOrders(ctx context.Context, limit int) ([]model.Order, error) {
	const getRecentSQL = `
    SELECT
      order_uid, track_number, entry,
      delivery, payment, items,
      locale, internal_signature, customer_id,
      delivery_service, shardkey, sm_id,
      date_created, oof_shard,
      created_at, updated_at, version
    FROM orders
    ORDER BY date_created DESC
    LIMIT $1
  `

	rows, err := p.pool.Query(ctx, getRecentSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("list recent orders: %w", err)
	}
	defer rows.Close()

	var orders []model.Order
	for rows.Next() {
		var (
			orderUID, trackNumber, entry          string
			locale, internalSignature, customerID string
			deliveryService, shardKey, oofShard   string
			deliveryJSON, paymentJSON, itemsJSON  []byte
			smID                                  int32
			dateCreated, createdAt, updatedAt     time.Time
			version                               int
		)

		if err := rows.Scan(
			&orderUID, &trackNumber, &entry,
			&deliveryJSON, &paymentJSON, &itemsJSON,
			&locale, &internalSignature, &customerID,
			&deliveryService, &shardKey, &smID,
			&dateCreated, &oofShard,
			&createdAt, &updatedAt, &version,
		); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		var o model.Order
		o.OrderUid = orderUID
		o.TrackNumber = trackNumber
		o.Entry = entry
		o.Locale = locale
		o.InternalSignature = internalSignature
		o.CustomerId = customerID
		o.DeliveryService = deliveryService
		o.ShardKey = shardKey
		o.SmId = int(smID)
		o.DateCreated = dateCreated
		o.OofShard = oofShard
		o.CreatedAt = createdAt
		o.UpdatedAt = updatedAt
		o.Version = version

		if err := json.Unmarshal(deliveryJSON, &o.Delivery); err != nil {
			return nil, fmt.Errorf("unmarshal delivery: %w", err)
		}
		if err := json.Unmarshal(paymentJSON, &o.Payment); err != nil {
			return nil, fmt.Errorf("unmarshal payment: %w", err)
		}
		if err := json.Unmarshal(itemsJSON, &o.Items); err != nil {
			return nil, fmt.Errorf("unmarshal items: %w", err)
		}

		orders = append(orders, o)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}

	return orders, nil
}
