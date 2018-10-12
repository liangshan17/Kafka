package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"sync"
	"encoding/json"
)

var (
    wg sync.WaitGroup
)

/*
*函数功能：从kafka服务器获取消息
*输入参数：消息主题topic
*/
func KafkaConsumerGetMessage(topic string){
	brokers := []string{"localhost:9092"}		//brokersIP地址: 192.168.130.143
	
	//1. config
	config :=sarama.NewConfig()
	config.Consumer.Return.Errors = true	//接收失败通知
	config.Version = sarama.V0_11_0_0	//版本
	
	//2. new Consumer
	consumer, err := sarama.NewConsumer(brokers, config)		
    if err != nil {
        fmt.Println("Failed to start consumer:",err)
        return
    }
    defer consumer.Close()
    
    //3. Message
    partitionList, err := consumer.Partitions(topic)		//获取指定Topic的所有分区Partitions
    if err != nil {
        fmt.Println("Failed to get the list of partitions:",err)
        return
    }
    
    var value Value	//message value
    
    //循环获取消息
    for partition := range partitionList {
	    //获取topic特定Partition特定Offset的消息, offset: sarama.OffsetNewest, sarama.OffsetOldest, offset
	    partitionConsumer, err := consumer.ConsumePartition(topic, int32(partition), 0)	
	    if err != nil {
	        fmt.Println("Failed to get partition consumer", err)
	        return
	    }
	    defer partitionConsumer.Close()
	    
	    go func(pc sarama.PartitionConsumer){
            wg.Add(1)
            for msg := range pc.Messages(){
            	json.Unmarshal(msg.Value, &value)	//message value
    	
		    	//do something 
    	     
		    	fmt.Printf("msg: topic=%s partition=%d offset=%d  timestamp=%s  ", msg.Topic, msg.Partition, msg.Offset, msg.Timestamp.Format("2006-Jan-02 15:04"))	//测试使用, 后期删除
		        fmt.Println("value=", value)//测试使用, 后期删除 
            }
            wg.Done()
        }(partitionConsumer)
	    
	}
    
    wg.Wait()
}

