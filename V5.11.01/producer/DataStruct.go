package main

import (

)

//在json中，小写成员无法导出，所以成员名大写开头
type Student struct{
	ID int32
	Name  string
}

type Value struct{
	ID int
	Name string
	Stu Student
}