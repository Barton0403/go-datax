package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/Barton0403/btgo-pkg/config"
	"github.com/Barton0403/go-datax/common"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/spf13/viper"
)

func main() {
	flag.Parse()

	if len(flag.Args()) < 1 {
		fmt.Println("no job file")
		return
	}

	config.ViperInit()
	mq := viper.GetStringMapString("mq")
	conn, err := amqp.Dial("amqp://" + mq["username"] + ":" + mq["password"] + "@" + mq["host"] + ":" + mq["port"] + "/")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	// 推入请求
	q, err := ch.QueueDeclare(common.QUEUE_DATAX_JOB, false, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	err = ch.PublishWithContext(context.Background(), "", q.Name, false, false, amqp.Publishing{
		ContentType:     "application/json",
		ContentEncoding: "UTF-8",
		Body:            []byte(flag.Arg(0)),
	})
	if err != nil {
		panic(err)
	}
}
