package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"github.com/nats-io/nats.go"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	url := flag.String("url", nats.DefaultURL, "NATS JS server URL")
	stream := flag.String("stream", "NATSDUPCHECK", "Stream name")
	replicas := flag.Int("replicas", 3, "Stream replicas")
	dupw := flag.Duration("dupw", 5*time.Second, "Stream duplicate window")
	n := flag.Int("n", 5, "Number of message sending threads")
	size := flag.Int("size", 32, "Message size")
	mindelay := flag.Duration("mindelay", 1*time.Millisecond, "Min delay before sending a new message")
	maxdelay := flag.Duration("maxdelay", 10*time.Millisecond, "Max delay before sending a new message")
	flag.Parse()

	var done int32
	isDone := func() bool { return atomic.LoadInt32(&done) != int32(0) }
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	go func() {
		<-quit
		atomic.StoreInt32(&done, int32(1))
	}()

	conn, err := nats.Connect(*url)
	fatalOn(err)
	js, err := conn.JetStream()
	fatalOn(err)
	_, err = js.StreamInfo(*stream)
	if err != nil {
		_, err = js.AddStream(&nats.StreamConfig{
			Name: *stream, NoAck: true, Replicas: *replicas, Duplicates: *dupw,
		})
		fatalOn(err)
	}
	conn.Close()

	fmt.Println("Press Ctrl+C to exit")

	var total int64
	buf := make([]byte, *size)
	c := 'a' + rand.Intn(26)
	for i := range buf {
		buf[i] = byte(c)
	}
	go func() {
		ticker := time.Tick(time.Second)
		for range ticker {
			fmt.Println(atomic.LoadInt64(&total))
		}
	}()
	var wg sync.WaitGroup
	wg.Add(*n)
	for i := 0; i < *n; i++ {
		go func() {
			defer wg.Done()
			conn, err := nats.Connect(*url)
			fatalOn(err)
			defer conn.Close()
			msg := nats.NewMsg(*stream)
			msg.Data = buf
			var id int64
			for id = 1; ; id++ {
				if isDone() {
					break
				}
				msg.Header.Set(nats.MsgIdHdr, strconv.FormatInt(id, 10))
				fatalOn(conn.PublishMsg(msg))
				atomic.AddInt64(&total, int64(1))
				delay := rand.Int63n(int64(*maxdelay))
				if delay < int64(*mindelay) {
					delay = int64(*mindelay)
				}
				time.Sleep(time.Duration(delay))
			}
		}()
	}
	wg.Wait()
}

func fatalOn(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
