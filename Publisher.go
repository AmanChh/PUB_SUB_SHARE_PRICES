package main

import (
    "context"
    "crypto/tls"
    "encoding/json"
    "fmt"
    "github.com/go-redis/redis/v8"
    "time"
)

type StockPrice struct {
    Symbol string  `json:"symbol"`
    Price  float64 `json:"price"`
}

func ConnectRedis() *redis.Client {
    opt, err := redis.ParseURL("redis://ibm_cloud_4c9e7cc5_2f91_465f_aebc_a29ec068f10c:6ebffd47568900c6d72a1b7a4eeab9213d3dc4711ad55fbcba953dec178e7e5c@a4cc9c6f-a0b6-48d2-ba94-5bf6e8aa4b99.co21lv7d0he2pp3gvq90.dev.databases.appdomain.cloud:31282/0")
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

func FetchStockPrices() ([]StockPrice, error) {
    // Replace with your logic to fetch stock prices from an API or source
    // This example simulates fetching prices for AAPL and GOOG
    return []StockPrice{
       {"AAPL", 150.23},
       {"GOOG", 2456.78},
    }, nil
}

func PublishStockPrices(client *redis.Client) error {
    prices, err := FetchStockPrices()
    if err != nil {
       return err
    }

    for _, price := range prices {
       message, err := json.Marshal(price) // Marshal data to JSON
       if err != nil {
          return err
       }
       channel := "stock_prices"
       err = client.Publish(context.Background(), channel, message).Err()
       if err != nil {
          return err
       }
       fmt.Printf("Published price update: %s - %.2f\n", price.Symbol, price.Price)
    }

    return nil
}
func main() {
    client := ConnectRedis()
    err := PublishStockPrices(client)
    time.Sleep(100)
    if err != nil {
       return
    }
}
