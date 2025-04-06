package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/IBM/sarama"
)

func main() {
	// 1. 配置消费者
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// 2. 创建消费者客户端
	brokers := []string{"47.76.77.29:9092"} // Kafka Broker 地址
	topic := "order-topic"
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("创建消费者失败: %v", err)
	}
	defer consumer.Close()

	// 3. 获取 Topic 的所有分区
	partitions, err := consumer.Partitions(topic)
	partitions = partitions[0:2]
	if err != nil {
		log.Fatalf("获取分区失败: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(len(partitions))

	// 4. 消费每个分区的消息
	for _, partition := range partitions {
		// 创建分区消费者（从最新偏移量开始消费）
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Printf("分区 %d 消费失败: %v", partition, err)
			continue
		}

		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			defer pc.Close()

			// 监听消息和错误
			for {
				select {
				case msg := <-pc.Messages():

					fmt.Printf("分区: %d, 偏移量: %d, 消息: %s\n",
						msg.Partition, msg.Offset, sarama.Encoder(msg.Value))
				case err := <-pc.Errors():
					log.Printf("消费错误: %v", err)
				}
			}
		}(pc)
	}

	// 等待中断信号（Ctrl+C）
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)
	<-sigchan

	wg.Wait()
}
