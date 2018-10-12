package main

import KafkaCluster "github.com/bsm/sarama-cluster"
import (
	"github.com/Shopify/sarama"
	"fmt"
	"time"
	"github.com/sdbaiguanghe/glog"
	"encoding/json"
)

func KafkaClusterConsumerGetMessage(topic string){
	brokers := []string{"localhost:9092"}
    topics := []string{topic}

	//1. config
    config := KafkaCluster.NewConfig()
    config.Consumer.Return.Errors = true
    config.Consumer.Offsets.CommitInterval=1*time.Second
    config.Consumer.Offsets.Initial=sarama.OffsetNewest
    config.Group.Return.Notifications = true

    //2. new Consumer
    consumer, err := KafkaCluster.NewConsumer(brokers, "consumer-group1", topics, config)
    if err != nil {
        fmt.Println("Failed to start consumer:",err)
    }
    defer consumer.Close()
    
    //3. Message
    go func(c *KafkaCluster.Consumer) {
        errors := c.Errors()
        noti := c.Notifications()
        for {
            select {
            case err := <-errors:
                glog.Errorln(err)
            case <-noti:
            }
        }
    }(consumer)

	var value Value	//message value
    for msg := range consumer.Messages() {		
    	json.Unmarshal(msg.Value, &value)	//message value
    	
    	//do something 
    	     
    	fmt.Printf("msg: topic=%s partition=%d offset=%d  ", msg.Topic, msg.Partition, msg.Offset)	//测试使用, 后期删除
        fmt.Println("value=", value)//测试使用, 后期删除
        
        consumer.MarkOffset(msg, "") //MarkOffset 并不是实时写入kafka，有可能在程序crash时丢掉未提交的offset
    }
}

