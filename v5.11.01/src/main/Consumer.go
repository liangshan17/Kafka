package main

import (
	"fmt"
	"github.com/Shopify/sarama"
)

type Data struct{
	data  []byte
}

type KafkaConsumer struct{
	kafkaIPAddr string
	msgChan chan *Data
}

func initKafkaConsumer(kafkaIPAddr string)(*KafkaConsumer){
	var chanSize int32 = 10000
	kc := &KafkaConsumer{
		kafkaIPAddr: kafkaIPAddr,
		msgChan : make(chan * Data, chanSize),
	}
	return kc
}

/*
*函数功能：从kafka服务器获取消息
*输入参数：消息主题topic
*/
func (k *KafkaConsumer)KafkaGetMassage(TOPIC string)(isOK bool , err error){
	consumer, err := sarama.NewConsumer([]string{k.kafkaIPAddr},nil)
	if err != nil{
		fmt.Println("failed to connect kafka, ", err)
		return false, err
	}
	partitionList, err := consumer.Partitions(TOPIC)
	if err != nil{
		fmt.Println("Failed to get the list of partitions, ", err)
		return false, err
	}
	for partition := range partitionList{
		pc, err := consumer.ConsumePartition(TOPIC, int32(partition), 0)
		if err != nil{
			fmt.Println("Failed to get the list of partitions, ", err)
			return false, err
		}
		
		go func(p sarama.PartitionConsumer) {
			for msg := range p.Messages() {
				
				data := &Data{
					data:msg.Value,
				}
				//将kafka消费的message扔到channel中
				k.msgChan <- data
			}
		}(pc)
	}
	
	return true, nil
}
