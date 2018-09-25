package main

import (
	"io/ioutil"
	"fmt"
)

func main(){
	sqlBytes, err := ioutil.ReadFile("docs/database/registerDb.sql");
	if err != nil {
		return
	}
	sqlTable := string(sqlBytes);
	fmt.Println("sqlTable",sqlTable)

}
