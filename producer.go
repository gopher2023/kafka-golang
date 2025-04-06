package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
	_ "os"
	"time"
)

func main() {
	// 1. 配置生产者
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true          // 确保消息发送成功
	config.Producer.RequiredAcks = sarama.WaitForAll // 等待所有副本确认

	// 2. 创建生产者客户端
	brokers := []string{"47.76.77.29:9092"} // Kafka Broker 地址
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("创建生产者失败: %v", err)
	}
	defer producer.Close()

	// 3. 发送消息到指定 Topic 和分区
	topic := "order-topic"

	for i := 0; i < 10000; i++ {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("消息内容 - %d", i)),
		}

		// 可选：指定分区（默认自动选择分区）
		// msg.Partition = 0
		// 发送消息
		time.Sleep(time.Millisecond * 100)
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("发送消息失败: %v", err)
		} else {
			fmt.Printf("消息发送成功 -> Topic: %s, 分区: %d, 偏移量: %d\n", topic, partition, offset)
		}

	}

}
