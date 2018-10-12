package main

import (
    "fmt"
    "github.com/Shopify/sarama"
)

/*
*函数功能：向kafka服务器发送消息
*输入参数：消息值value,消息主题topic
*返回值：发送成功标志isOK, 消息partition, 消息offset
*/
func KafkaAsyncProducerSendMessage(value []byte, topic string) (isOK bool , partition int32, offset int64) {
	brokers := []string{"localhost:9092"}		//brokersIP地址: 192.168.130.143
	
	 //1. config
    config := sarama.NewConfig()	
    config.Producer.RequiredAcks = sarama.WaitForAll	//等待服务器所有副本都保存成功后的响应
    config.Producer.Partitioner = sarama.NewRandomPartitioner	//随机向partition发送消息sarama.NewRandomPartitioner
    config.Producer.Return.Successes = true	//是否等待成功和失败后的响应,只有上面的RequireAcks设置不是NoReponse这里才有用.
    config.Producer.Return.Errors = true
    config.Version = sarama.V0_10_0_1
 
    //2. new Produer
    producer, err := sarama.NewAsyncProducer(brokers, config)	//192.168.130.143
    if err != nil {
        fmt.Println("Failed to start Producer:",err)
        return
    }
    defer producer.AsyncClose()
    
    //3. Message
    message := &sarama.ProducerMessage{
	   	Topic: topic,						//message.topic
	    Value: sarama.ByteEncoder(value),	//message.Value
	}
	
	//4. send Message
	//使用通道发送
    producer.Input() <- message
    
    select {
    	case suc := <-producer.Successes():		//接收成功通道
	    	fmt.Println("partition:", suc.Partition, "offset:", suc.Offset)  //测试使用, 后期删除
			return true, suc.Partition, suc.Offset
		case fail := <-producer.Errors():		//接收失败通道
	        fmt.Println("err: ", fail.Err)	
	        return false, -1, -1
   }	 
}