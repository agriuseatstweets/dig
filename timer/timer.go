package main

import (
	"time"
	"log"
	"fmt"
	"github.com/caarlos0/env/v6"
	"github.com/agriuseatstweets/go-pubbers/pubbers"
)

const (
	CONFIG_FORMAT = "2006-01-02"
	MESSAGE_FORMAT = "2006-01-02"
)

type CatchupConfig struct {
	Start string `env:"DIGTIMER_START,required"`
	Finish string `env:"DIGTIMER_FINISH,required"`
}

type DelayConfig struct {
	Delay int `env:"DIGTIMER_DELAY,required"` // days
}

func getConfig() interface{} {
	dc := DelayConfig{}
	delayErr := env.Parse(&dc)

	cc := CatchupConfig{}
	catchupErr := env.Parse(&cc)

	if delayErr != nil && catchupErr != nil {
		panic(delayErr)
	}

	if delayErr == nil && catchupErr == nil {
		panic("Environment for DigTimer overdetermined! Must be catchup or delay")
	}

	if delayErr == nil {
		return dc
	}
	return cc
}

func key() []byte {
	return []byte(time.Now().Format(time.RFC3339))
}


func makeRange(start, finish time.Time) []string {
	start = start.Truncate(24*time.Hour)
	finish = finish.Truncate(24*time.Hour)

	dates := []string{start.Format(MESSAGE_FORMAT)}

	for start.Before(finish) {
		start = start.AddDate(0, 0, 1)
		dates = append(dates, start.Format(MESSAGE_FORMAT))
	}

	return dates
}

func runFrom(start, finish time.Time) chan pubbers.QueuedMessage {
	ch := make(chan pubbers.QueuedMessage)
	go func() {
		defer close(ch)
		for _, d := range makeRange(start, finish) {
			ch <- pubbers.QueuedMessage{key(), []byte(d)}
		}
	}()
	return ch
}

func runDelay(cnf DelayConfig) chan pubbers.QueuedMessage {
	t := time.Now()
	start := t.AddDate(0, 0, -cnf.Delay)

	return runFrom(start, start)
}

func runCatchup(cnf CatchupConfig) chan pubbers.QueuedMessage {
	start, err := time.Parse(CONFIG_FORMAT, cnf.Start)
	if err != nil {
		panic(err)
	}
	finish, err := time.Parse(CONFIG_FORMAT, cnf.Finish)
	if err != nil {
		panic(err)
	}

	return runFrom(start, finish)
}


func monitor(errs <-chan error) {
	e := <- errs
	log.Fatalf("DigTimer failed with error: %v", e)
}


func main() {
	errs := make(chan error)
	go monitor(errs)

	writer, err := pubbers.NewKafkaWriter()
	if err != nil {
		errs <- err
	}

	var messages chan pubbers.QueuedMessage

	switch cnf := getConfig().(type) {
	case DelayConfig:
		messages = runDelay(cnf)
	case CatchupConfig:
		messages = runCatchup(cnf)
	}

	res := writer.Publish(messages, errs)

	if res.Sent != res.Written {
		panic(fmt.Sprintf("DigTimer did not send all results. %v sent and %v written", res.Sent, res.Written))
	}

	log.Printf("DigTimer finished with %v times and %v written", res.Sent, res.Written)
}
