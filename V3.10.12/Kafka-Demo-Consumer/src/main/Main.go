package main

import (

)

func main() {
	var TOPIC string = "TEST"
	
	//KafkaConsumerGetMessage(TOPIC)	//获取该TOPIC下的所有消息
	KafkaClusterConsumerGetMessage(TOPIC)	//获取该TOPIC下的最新消息
}