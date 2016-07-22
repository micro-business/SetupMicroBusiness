package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

var cassandraHosts string
var cassandraProtoclVersion int

func main() {
	flag.StringVar(&cassandraHosts, "cassandra-hosts", "127.0.0.1", "The list of cassandra hosts to connect to. The default value is 127.0.0.1")
	flag.IntVar(&cassandraProtoclVersion, "cassandra-protocl-version", 4, "The cassandra protocl version. The default value is 4.")
	flag.Parse()

	cluster := gocql.NewCluster()
	cluster.Hosts = strings.Split(cassandraHosts, ",")
	cluster.ProtoVersion = cassandraProtoclVersion
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 10 * time.Second

	session, err := cluster.CreateSession()

	if err != nil {
		log.Fatal(err.Error())

		return
	}

	urls := [4]string{
		"https://raw.githubusercontent.com/microbusinesses/AddressService/master/DatabaseScript.cql",
		"https://raw.githubusercontent.com/microbusinesses/UserService/master/DatabaseScript.cql",
		"https://raw.githubusercontent.com/microbusinesses/NameService/master/DatabaseScript.cql",
		"https://raw.githubusercontent.com/microbusinesses/PersonService/master/DatabaseScript.cql"}

	errorChannel := make(chan error, len(urls))

	var waitGroup sync.WaitGroup

	for _, url := range urls {
		waitGroup.Add(1)

		go runCqlScript(session, errorChannel, &waitGroup, url)
	}

	go func() {
		waitGroup.Wait()
		close(errorChannel)
	}()

	errorMessage := ""
	errorFound := false

	for err := range errorChannel {
		if err != nil {
			errorMessage += err.Error()
			errorMessage += "\n"
			errorFound = true
		}
	}

	if errorFound {
		log.Fatal(errorMessage)
	}
}

func runCqlScript(session *gocql.Session, errorChannel chan<- error, waitGroup *sync.WaitGroup, url string) {
	defer waitGroup.Done()

	resp, err := http.Get(url)

	if err != nil {
		errorChannel <- err

		return
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		errorChannel <- err

		return
	}

	scriptLines := strings.Split(string(body[:len(body)]), "\n")

	for _, scriptLine := range scriptLines {
		scriptLine = strings.TrimSpace(scriptLine)

		if len(scriptLine) != 0 {
			fmt.Println("Running command: " + scriptLine)

			err = session.Query(scriptLine).Exec()

			if err != nil {
				errorChannel <- err

				return
			}
		}

	}

	errorChannel <- nil
}
