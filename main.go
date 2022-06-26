package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/spudtrooper/goutil/check"
	goutilio "github.com/spudtrooper/goutil/io"
	"github.com/spudtrooper/goutil/must"
)

var (
	imagesDir   = flag.String("images_dir", "data/downloaded", "images output directory")
	threads     = flag.Int("threads", 20, "number of download threads")
	opendataDir = flag.String("opendata_dir", "../opendata", "path to the https://github.com/NationalGalleryOfArt/opendata base dir")
)

type painting struct {
	id            int
	artist, title string
	mainURI       string
}

func findPaintings() []*painting {
	f, err := os.Open(path.Join(*opendataDir, "data/objects.csv"))
	check.Err(err)
	defer f.Close()
	csvReader := csv.NewReader(f)
	csvReader.Comma = ','
	data, err := csvReader.ReadAll()
	check.Err(err)
	var paintings []*painting
	for _, line := range data[1:] {
		if isPainting := line[17] == "Painting"; isPainting {
			id, title, artist := must.Atoi(line[0]), line[4], line[14]
			p := &painting{
				id:     id,
				artist: artist,
				title:  title,
			}
			paintings = append(paintings, p)
		}
	}
	return paintings
}

func addUris(paintings []*painting) {
	pmap := map[int]*painting{}
	for _, p := range paintings {
		pmap[p.id] = p
	}
	f, err := os.Open(path.Join(*opendataDir, "data/published_images.csv"))
	check.Err(err)
	defer f.Close()
	csvReader := csv.NewReader(f)
	csvReader.Comma = ','
	data, err := csvReader.ReadAll()
	check.Err(err)
	for _, line := range data[1:] {
		id := must.Atoi(line[10])
		if p, found := pmap[id]; found {
			mainURI := line[2]
			p.mainURI = mainURI
		}
	}
}

func downloadFile(URL, fileName string) error {
	//Get the response bytes from the url
	response, err := http.Get(URL)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return errors.Errorf("Received non 200 response code")
	}
	//Create a empty file
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	//Write the bytes to the fiel
	_, err = io.Copy(file, response.Body)
	if err != nil {
		return err
	}

	return nil
}

func realMain() {
	paintings := findPaintings()
	addUris(paintings)
	goutilio.MkdirAll(*imagesDir)
	log.Printf("have %d paintings", len(paintings))

	paintingsCh := make(chan *painting)
	go func() {
		for _, p := range paintings {
			paintingsCh <- p
		}
		close(paintingsCh)
	}()

	type dlStats struct {
		existed, err, noURI, downloaded int64
	}

	var stats dlStats

	var wg sync.WaitGroup
	for i := 0; i < *threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range paintingsCh {
				if p.mainURI == "" {
					log.Printf("no URI for %v", p)
					atomic.AddInt64(&stats.noURI, 1)
					continue
				}
				f := path.Join(*imagesDir, fmt.Sprintf("%d.jpg", p.id))
				if goutilio.FileExists(f) {
					atomic.AddInt64(&stats.existed, 1)
					continue
				}
				uri := p.mainURI
				uri = strings.Replace(uri, "200,200", "1600,1600", 1)
				log.Printf("downloading %s -> %s", uri, f)
				if err := downloadFile(uri, f); err != nil {
					log.Printf("error: %v", err)
					atomic.AddInt64(&stats.err, 1)
				}
				atomic.AddInt64(&stats.downloaded, 1)
			}
		}()
	}
	wg.Wait()

	log.Printf("stats: %+v", stats)
}

func main() {
	flag.Parse()
	realMain()
}
