package main

import (
	"bufio"
	"context"
	"github.com/NavExplorer/navexplorer-api-go/elasticsearch"
	"github.com/NavExplorer/node-indexer/config"
	"github.com/fsnotify/fsnotify"
	"github.com/olivere/elastic"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func waitUntilFind(filename string) error {
	for {
		time.Sleep(1 * time.Second)
		_, err := os.Stat(filename)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			} else {
				return err
			}
		}
		break
	}
	return nil
}

func main() {
	log.Println("Starting node indexer")
	filename := config.Get().SeedFile

	// parse the file on first load
	parse()

	err := waitUntilFind(filename)
	if err != nil {
		log.Fatalln(err)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalln(err)
	}
	defer watcher.Close()

	err = watcher.Add(filename)
	if err != nil {
		log.Fatalln(err)
	}

	renameCh := make(chan bool)
	removeCh := make(chan bool)
	errCh := make(chan error)

	go func() {
		for {
			select {
			case event := <-watcher.Events:
				switch {
				case event.Op&fsnotify.Write == fsnotify.Write:
					log.Printf("Write:  %s: %s", event.Op, event.Name)
					parse()
				case event.Op&fsnotify.Create == fsnotify.Create:
					log.Printf("Create: %s: %s", event.Op, event.Name)
				case event.Op&fsnotify.Rename == fsnotify.Rename:
					log.Printf("Rename: %s: %s", event.Op, event.Name)
					parse()
					renameCh <- true
				}
			case err := <-watcher.Errors:
				errCh <- err
			}
		}
	}()

	go func() {
		for {
			select {
			case <-renameCh:
				err = waitUntilFind(filename)
				if err != nil {
					log.Fatalln(err)
				}
				err = watcher.Add(filename)
				if err != nil {
					log.Fatalln(err)
				}
			case <-removeCh:
				err = waitUntilFind(filename)
				if err != nil {
					log.Fatalln(err)
				}
				err = watcher.Add(filename)
				if err != nil {
					log.Fatalln(err)
				}
			}
		}
	}()

	log.Fatalln(<-errCh)
}

func parse() {
	log.Printf("Parsing file: %s", config.Get().SeedFile)
	file, err := os.Open(config.Get().SeedFile)
	if err != nil {
		log.Println("Failed to locate dump")
		return
	}
	defer file.Close()

	var nodes []Node

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		log.Printf("Working on line: %s\n", line)

		words := strings.Fields(line)
		if words[0] != "#" && len(words) == 12 {
			log.Printf("Loading Node: %s", words[0])

			var node Node
			node.Address = words[0]
			node.Good = words[1] == "1"
			i, err := strconv.ParseInt(words[2], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			node.LastSuccess = time.Unix(i, 0)

			percentReg, err := regexp.Compile("[^0-9.]+")
			if err != nil {
				log.Fatal(err)
			}

			percent2h, err := strconv.ParseFloat(percentReg.ReplaceAllString(words[3], ""), 64)
			if err != nil {
				log.Fatal(err)
			}
			node.Percent2h = percent2h

			percent8h, err := strconv.ParseFloat(percentReg.ReplaceAllString(words[4], ""), 64)
			if err != nil {
				log.Fatal(err)
			}
			node.Percent8h = percent8h

			percent1d, err := strconv.ParseFloat(percentReg.ReplaceAllString(words[5], ""), 64)
			if err != nil {
				log.Fatal(err)
			}
			node.Percent1d = percent1d

			percent7d, err := strconv.ParseFloat(percentReg.ReplaceAllString(words[6], ""), 64)
			if err != nil {
				log.Fatal(err)
			}
			node.Percent7d = percent7d

			percent30d, err := strconv.ParseFloat(percentReg.ReplaceAllString(words[7], ""), 64)
			if err != nil {
				log.Fatal(err)
			}
			node.Percent30d = percent30d

			node.Blocks, err = strconv.ParseInt(words[8], 10, 64)
			node.Svcs = words[9]
			node.Version = words[10]
			node.UserAgent = strings.Trim(words[11], "\"/")

			userAgentVersion, err := regexp.Compile("[^0-9.]+")
			if err != nil {
				log.Fatal(err)
			}
			node.UserAgentVersion = userAgentVersion.ReplaceAllString(node.UserAgent, "")

			nodes = append(nodes, node)
		} else {
			log.Printf("Found %d words on line", len(words))
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Found %d nodes", len(nodes))

	client, err := elasticsearch.NewClient()
	if err != nil {
		log.Fatal(err)
	}
	mapping, err := ioutil.ReadFile("node.json")
	if err != nil {
		log.Fatal(err)
	}

	if len(nodes) == 0 {
		log.Println("Didnt find any nodes...")
		return
	}

	ctx := context.Background()
	deleteIndex, _ := client.DeleteIndex("mainnet.nodes").Do(ctx)
	if deleteIndex != nil && deleteIndex.Acknowledged {
		log.Println("Deleted index")
	}
	createIndex, err := client.CreateIndex("mainnet.nodes").BodyString(string(mapping)).Do(ctx)
	if !createIndex.Acknowledged {
		log.Fatal("FATAL", "Failed to create temp index")
	}

	bulkRequest := client.Bulk()
	for i := range nodes {
		log.Printf("Indexing node %s\n", nodes[i].Address)
		req := elastic.NewBulkIndexRequest().Index("mainnet.nodes").Type("_doc").Id(nodes[i].Address).Doc(nodes[i])
		bulkRequest = bulkRequest.Add(req)
	}
	bulkResponse, err := bulkRequest.Do(ctx)
	if err != nil {
		log.Println("LOG: ", err)
	}
	if bulkResponse.Errors {
		log.Fatal("FATAL", "Error performing bulk insert")
	}
}

type Node struct {
	Address          string    `json:"address"`
	Good             bool      `json:"good"`
	LastSuccess      time.Time `json:"lastSuccess"`
	Percent2h        float64   `json:"percent2h"`
	Percent8h        float64   `json:"percent8h"`
	Percent1d        float64   `json:"percent1d"`
	Percent7d        float64   `json:"percent7d"`
	Percent30d       float64   `json:"percent30d"`
	Blocks           int64     `json:"blocks"`
	Svcs             string    `json:"svcs"`
	Version          string    `json:"version"`
	UserAgent        string    `json:"userAgent"`
	UserAgentVersion string    `json:"userAgentVersion"`
}
