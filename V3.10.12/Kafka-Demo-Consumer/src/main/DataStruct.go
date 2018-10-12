package main

import (

)

//在json中，小写成员无法导出，所以成员名大写开头
type Value struct{
	ID int
	Name string
}