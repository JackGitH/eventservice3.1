package main

import (
	"github.com/golang/protobuf/proto"
	"log"
	"protocdemo/example"
)

func main() {
	// 为 AllPerson 填充数据
	p1 := example.Person{
		Id:   *proto.Int32(1),
		Name: *proto.String("xieyanke"),
	}

	p2 := example.Person{
		Id:   2,
		Name: "gopher",
	}

	all_p := example.AllPerson{
		Per: []*example.Person{&p1, &p2},
	}

	// 对数据进行序列化
	data, err := proto.Marshal(&all_p)
	if err != nil {
		log.Fatalln("Mashal data error:", err)
	}

	// 对已经序列化的数据进行反序列化
	var target example.AllPerson
	err = proto.Unmarshal(data, &target)
	if err != nil {
		log.Fatalln("UnMashal data error:", err)
	}

	println(target.Per[0].Name,target.Per[1].Name) // 打印第一个 person Name 的值进行反序列化验证
}
