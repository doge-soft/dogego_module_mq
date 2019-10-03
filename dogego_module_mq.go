package dogego_module_mq

import (
	"fmt"
	"github.com/go-redis/redis"
	"log"
)

type RedisMQ struct {
	RedisClient *redis.Client
}

func NewRedisMQ(redis_client *redis.Client) *RedisMQ {
	return &RedisMQ{
		RedisClient: redis_client,
	}
}

func (mq *RedisMQ) Publish(queuename string, message string) error {
	err := mq.RedisClient.LPush(
		fmt.Sprintf("queue:%s", queuename), message).Err()

	if err != nil {
		return err
	}

	return nil
}

func (mq *RedisMQ) Custome(queuename string, cb func(message string) error) error {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Println(err)
			}
		}()

		for {
			message, err := mq.RedisClient.BRPop(0,
				fmt.Sprintf("queue:%s", queuename)).Result()

			if err != nil {
				log.Println(err)
			}

			err = cb(message[1])

			if err != nil {
				log.Printf("Execute Callback func Error: %s", err)
			}
		}
	}()

	return nil
}
