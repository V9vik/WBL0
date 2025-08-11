package cache

import (
	model "WBTests/internal/Models"
	"sync"
)

type Cache struct {
	mu sync.RWMutex
	m  map[string]model.Order
}

func New() *Cache {
	return &Cache{
		m: make(map[string]model.Order),
	}
}

func (c *Cache) Get(id string) (model.Order, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.m[id]
	return v, ok
}

func (c *Cache) Set(id string, o model.Order) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[id] = o
}

func (c *Cache) Delete(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, id)
}

func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.m)
}
