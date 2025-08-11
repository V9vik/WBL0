package store

import (
	model "WBTests/internal/Models"
	"context"
	"errors"
)

type Store interface {
	UpsertOrder(ctx context.Context, o model.Order) error
	GetOrder(ctx context.Context, id string) (model.Order, error)
	Close()
}

var ErrNotFound = errors.New("not found")
