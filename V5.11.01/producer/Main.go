package main

import (
	"fmt"
	"encoding/json"
)

func main() {
	var TOPIC string = "TEST"
	var kafkaIPAddr string = "192.168.130.143:9092"
	var kafkaAsyncProducer *KafkaAsyncProducer
	
	//Data
	value := Value{2018,"liangshan", Student{11, "data"}}
	data, _ := json.Marshal(value)	//json
	
	kafkaAsyncProducer = initKafkaAsyncProducer(kafkaIPAddr)	//1. 初始化
	isOK, err := kafkaAsyncProducer.KafkaSendMessage(data, TOPIC) //2.send
	
	if(!isOK){
		fmt.Println("err: ", err)
	}
}