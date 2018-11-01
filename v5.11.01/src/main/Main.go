package main

import (
	"fmt"
	"encoding/json"
	"sync"
)

var waitGroup sync.WaitGroup

func main() {
	var TOPIC string = "TEST-LS"
	var kafkaIPAddr string = "192.168.130.143:9092"
	var kafkaConsumer *KafkaConsumer
	
	kafkaConsumer = initKafkaConsumer(kafkaIPAddr)	//1.kafkaConumer初始化
	isOK, err := kafkaConsumer.KafkaGetMassage(TOPIC)	//2. 获取消息
	if isOK {	
		var value Value
		
		for msg := range kafkaConsumer.msgChan{//Message
			json.Unmarshal(msg.data, &value)	//message value
		
			//3.消息处理
			fmt.Println("value=", value)//测试使用, 后期删除 
		}
		//waitGroup.Done()
	}else{
		fmt.Println("error:, ", err)
	}
}