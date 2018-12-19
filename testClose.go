package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

const (
	attemptedMurderNum = 5           // we'll attempt to kill the body this many times
	outputPrefix       = ">>>>>>>>>" // prepend this so that we can more easily see this program's output vs the SDK logs

	// a URL pointing to a large blob
	largeBlobURL = "BLOB_URL"
)

func main() {
	// get the retry reader to the HTTP body
	bodyReader := performDownload().Body(azblob.RetryReaderOptions{
		MaxRetryRequests: 3, // this number of retries for every read operation
	})

	// we'll download the body into memory
	downloadedData := &bytes.Buffer{}

	// a wait group is used to coordinate with the go routine reading the body continuously
	wg := sync.WaitGroup{}
	wg.Add(1)

	//
	go func() {
		output(fmt.Sprintf("%s Started reading body", time.Now().Format(time.RFC3339)))
		n, err := downloadedData.ReadFrom(bodyReader)
		output(fmt.Sprintf("%s Stopped reading body", time.Now().Format(time.RFC3339)))
		output(fmt.Sprintf("%v bytes were read, %s error was raised", n, err))

		wg.Done()
	}()

	for n := int64(0); n <= attemptedMurderNum; n++ {
		// sleep for a certain amount of time
		time.Sleep(time.Duration(n+2) * time.Second)

		// cut off the reader
		bodyReader.Close()
		output(fmt.Sprintf("%s Closed body reader for the %vth time", time.Now().Format(time.RFC3339), n))
	}

	wg.Wait()
}

func output(msg string) {
	fmt.Printf("%s %s\n", outputPrefix, msg)
}

// perform a simple download of a blob that's relatively large so that we can trigger multiple retries
func performDownload() *azblob.DownloadResponse {
	// write logs to the stdout
	logger := log.New(os.Stdout, "", log.Ldate|log.Lmicroseconds)

	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			TryTimeout: 50 * time.Hour,
		},
		Log: pipeline.LogOptions{
			Log: func(s pipeline.LogLevel, m string) {
				logger.Output(2, m)
			},
			ShouldLog: func(level pipeline.LogLevel) bool {
				return level <= pipeline.LogInfo
			},
		},
	})

	u, _ := url.Parse(largeBlobURL)
	blobURL := azblob.NewBlobURL(*u, p)

	// Download the blob's contents and verify that it worked correctly
	get, err := blobURL.Download(context.Background(), 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		log.Fatal(err)
	}

	return get
}
