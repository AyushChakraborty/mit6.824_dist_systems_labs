package main

import (
	"fmt"
	"sync"
)

type Fetcher interface {
	Fetch(url string) (urls []string, err error)
}

// goal is to implement a rough version of a crawler, so we build a dummy site with a body and urls in the site
type dummySite struct {
	body string
	urls []string
}

// dummy adjacency list of the sites
type dummyGraph map[string]*dummySite

func (d dummyGraph) Fetch(url string) ([]string, error) {
	if res, ok := d[url]; ok {
		fmt.Printf("url found: %s\n", url)
		return res.urls, nil
	}
	fmt.Printf("url missing: %s\n", url)
	return nil, fmt.Errorf("not found: %s\n", url)
}

// populating the dummy adj list
var dummy = dummyGraph{
	"http://golang.org/": &dummySite{
		"official go docs here", []string{
			"http://golang.org/pkg/",
			"http://golang.org/cmd/",
		},
	},
	"http://golang.org/pkg/": &dummySite{
		"Packages",
		[]string{
			"http://golang.org/",
			"http://golang.org/cmd/",
			"http://golang.org/pkg/fmt/",
			"http://golang.org/pkg/os/",
		},
	},
	"http://golang.org/pkg/fmt/": &dummySite{
		"Package fmt",
		[]string{
			"http://golang.org/",
			"http://golang.org/pkg/",
		},
	},
	"http://golang.org/pkg/os/": &dummySite{
		"Package os",
		[]string{
			"http://golang.org/",
			"http://golang.org/pkg/",
		},
	},
}

// serial crawler
func Serial(fetched map[string]bool, url string, fetcher Fetcher) {
	if fetched[url] {
		return
	}
	fetched[url] = true
	urls, err := fetcher.Fetch(url)
	if err != nil {
		return
	}

	for _, u := range urls {
		Serial(fetched, u, fetcher)
	}
}

// concurrecy v1, shared memory with mutex
type sharedMem struct {
	mu      sync.Mutex
	fetched map[string]bool
}

func makeSharedMem() *sharedMem {
	s := &sharedMem{}
	s.fetched = make(map[string]bool)
	return s
}

func ConcSharedMem(shared *sharedMem, url string, fetcher Fetcher) {
	shared.mu.Lock()
	already := shared.fetched[url]
	shared.fetched[url] = true
	shared.mu.Unlock()

	if already {
		return
	}

	urls, err := fetcher.Fetch(url)
	if err != nil {
		return
	}

	var done sync.WaitGroup
	for _, u := range urls {
		done.Add(1)
		go func(u string) {
			defer done.Done()
			ConcSharedMem(shared, u, fetcher)
		}(u)
	}
	done.Wait()
}

// concurrency v2, message passing
func worker(ch chan []string, url string, fetcher Fetcher) {
	urls, err := fetcher.Fetch(url)
	if err != nil {
		ch <- []string{}
	} else {
		ch <- urls
	}
}

func master(ch chan []string, fetcher Fetcher) {
	n := 1
	fetched := make(map[string]bool)
	for urls := range ch {
		for _, u := range urls {
			if fetched[u] == false {
				fetched[u] = true
				n += 1
				go worker(ch, u, fetcher)
			}
		}
		n -= 1
		if n == 0 { //without this, there will be no way to exit the loop if channel is empty, and go presents this as a deadlock
			break
		}
	}
}

func ConcMessagePass(url string, fetcher Fetcher) {
	ch := make(chan []string)
	go func() {
		ch <- []string{url}
	}()
	master(ch, fetcher)
}

func main() {
	fmt.Println("hello")

	fmt.Print("serial\n")
	Serial(make(map[string]bool), "http://golang.org/", dummy)

	fmt.Print("concurrent shared memory\n")
	ConcSharedMem(makeSharedMem(), "http://golang.org/", dummy)

	fmt.Print("concurrent message passing\n")
	ConcMessagePass("http://golang.org/", dummy)

}
