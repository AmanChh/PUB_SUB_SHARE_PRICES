package main

import (
    "context"
    "crypto/tls"
    "encoding/json"
    "fmt"
    "github.com/go-redis/redis/v8"
    "log"
)

type StockPrice struct {
    Symbol string  `json:"symbol"`
    Price  float64 `json:"price"`
}

func ConnectRedis() *redis.Client {
    opt, err := redis.ParseURL("redis://<url>/0")
    if err != nil {
       panic(err)
    }

    opt.TLSConfig = &tls.Config{InsecureSkipVerify: true}
    rdb := redis.NewClient(opt)
    if err := rdb.Ping(context.Background()).Err(); err != nil {
       fmt.Println(err)
    }
    return rdb
}

func SubscribeToPrices(client *redis.Client) {
    channel := "stock_prices"
    fmt.Println("Subscribe to stock_prices channel")
    pubsub := client.Subscribe(context.Background(), channel)

    for {
       msg, err := pubsub.ReceiveMessage(context.Background())
       if err != nil {
          log.Println(err)
          return
       }

       var price StockPrice
       err = json.Unmarshal([]byte(msg.Payload), &price)
       if err != nil {
          log.Println(err)
          continue
       }
       fmt.Printf("Received price update: %s - %.2f\n", price.Symbol, price.Price)
    }
}

func main() {
    client := ConnectRedis()
    SubscribeToPrices(client)
}
