package main

import (
	"fmt"
	"github.com/Barton0403/btgo-pkg/config"
	"github.com/Barton0403/go-datax/common"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/spf13/viper"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
)

func main() {
	errChan := make(chan error)

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

	q, _ := ch.QueueDeclare(common.QUEUE_DATAX_JOB, false, false, false, false, nil)
	msgs, _ := ch.Consume(q.Name, "", true, false, false, false, nil)

	go func() {
		for d := range msgs {
			fmt.Println(string(d.Body))
			args := common.BuildJavaArgs(string(d.Body), -1, "standalone")
			c := exec.Command("java", args...)
			c.Stdout = os.Stdout
			c.Stderr = os.Stderr
			e := c.Run()
			if e != nil {
				log.Fatal(e)
			}
		}
	}()

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errChan <- fmt.Errorf("%s", <-c)
	}()

	error := <-errChan
	fmt.Println(error)
}
