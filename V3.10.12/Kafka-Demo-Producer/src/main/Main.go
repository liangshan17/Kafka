package main

import (
	"fmt"
	"encoding/json"
)

func main() {
	var TOPIC string = "TEST"
	
	value := Value{2018,"liangshan"}
	data, _ := json.Marshal(value)
	
	var isOK bool
	
	isOK, _, _ = KafkaAsyncProducerSendMessage(data, TOPIC) //异步
	
	fmt.Println("message send ", isOK)	//测试使用，后期删除
}