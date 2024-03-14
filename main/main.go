package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"sync"
	"time"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	_, err := rdb.Ping(context.Background()).Result() //检验是否链接成功
	if err != nil {
		log.Println(err) //输出错误日志
		return
	}
	log.Println("Connect redis success")

	publish := rdb.Subscribe(context.Background(), "channel") //订阅模型

	ch := publish.Channel() //将信息传到管道
	stop := make(chan bool) //设置一个停止信号

	var wg sync.WaitGroup
	wg.Add(2) // 一个等待消息接收的goroutine，一个等待消息发送的goroutine

	go func() { // 接收消息的goroutine
		defer wg.Done() // 任务完成后通知WaitGroup

		for {
			select {
			case msg, ok := <-ch: //检测管道是否有信息
				if !ok {
					return
				}
				fmt.Println("the", msg.Channel, "sent", msg.Payload, "successfully")
				message := msg.Payload
				err = rdb.ZAdd(context.Background(), "channel", redis.Z{Score: float64(time.Now().Unix()), Member: message}).Err() //将传输的信息添加到有序集合中
				if err != nil {
					log.Println(err) //输出错误日志
					return
				}
			case <-stop: //检测停止信号
				err := publish.Unsubscribe(context.Background(), "channel") //停止订阅
				if err != nil {
					log.Println(err) //输出错误日志
					return
				} else {
					fmt.Println("Unsubscribe success")
					return
				}
			}
		}
	}()

	go func() { // 发送消息的goroutine
		defer wg.Done() // 任务完成后通知WaitGroup
		for {

			var message string
			fmt.Println("Enter message to publish (Enter 'exit' :to exit )")

			fmt.Scanln(&message) // 读取用户输入的信息

			if message == "exit" {
				close(stop) // 发送停止信号
				return
			}

			err = rdb.Publish(context.Background(), "channel", message).Err() // 将信息发送到订阅模型
			if err != nil {
				log.Println(err) //输出错误日志
				return
			}
		}
	}()

	wg.Wait() // 等待两个goroutine完成后再退出

	messages, err := rdb.ZRevRangeWithScores(context.Background(), "channel", 0, -1).Result() //将有序集合中的信息读取出来
	if err != nil {
		log.Println(err) //输出错误日志
		return
	} else {
		fmt.Println("Messages sent:")
		for _, msg := range messages {
			message := msg.Member
			score := msg.Score
			times := time.Unix(int64(score), 0)
			fmt.Println(times, "the message is:", message)
		}
	}

	defer publish.Close() //关闭发布者
}
