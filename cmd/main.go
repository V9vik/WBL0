package main

import (
	"WBTests/internal/cache"
	"WBTests/internal/config"
	httpapi "WBTests/internal/http"
	kqueue "WBTests/internal/queue/kafka"
	"WBTests/internal/store"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(cfg.DBSN, "is started")

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	storage, err := store.NewPGStore(ctx, cfg.DBSN)
	if err != nil {
		log.Fatal(err)
	}
	defer storage.Close()
	orders, err := storage.ListRecentOrders(ctx, 1000)
	cah := cache.New()
	if err != nil {
		log.Printf("cache warm-up error: %v", err)
	} else {
		for _, o := range orders {
			cah.Set(o.OrderUid, o)
		}
		log.Printf("cache warm-up: %d orders", len(orders))
	}

	brokersEnv := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	topic := strings.TrimSpace(os.Getenv("KAFKA_TOPIC"))
	group := strings.TrimSpace(os.Getenv("KAFKA_GROUP"))

	var brokers []string
	if brokersEnv == "" {
		brokers = []string{"localhost:9092"}
	} else {
		parts := strings.Split(brokersEnv, ",")
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				brokers = append(brokers, p)
			}
		}
		if len(brokers) == 0 {
			brokers = []string{"localhost:9092"}
		}
	}
	if topic == "" {
		topic = "orders"
	}
	if group == "" {
		group = "orders-consumer"
	}

	// впринципе можем и убрать в отдельный файл
	kCfg := kqueue.Config{
		Brokers: brokers,
		Topic:   topic,
		GroupID: group,
	}

	cons, err := kqueue.NewConsumer(kCfg, storage, cah)

	if err != nil {
		log.Fatal(err)
	}

	go func() {
		if err := cons.Run(ctx); err != nil {
			log.Printf("consumer stopped: %v", err)
		}
	}()

	fmt.Println(storage, "is ok")

	mux := http.NewServeMux()

	mux.Handle("/", http.FileServer(http.Dir("web")))

	h := httpapi.NewHandler(storage, cah)

	h.Routes(mux)

	srv := &http.Server{
		Addr:    ":8081",
		Handler: mux,
	}

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %s\n", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
	time.Sleep(300 * time.Millisecond)
	storage.Close()
}
