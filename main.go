package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/jessevdk/go-flags"
	"github.com/ryuichi1208/ral2jqlog-lambda/lib/s3"
)

type options struct {
	DST_BUCKET string `short:"d" long:"dst-bucket" description:"audit log file" required:"false"`
	SRC_BUCKET string `short:"s" long:"src-bucket" description:"File Content Type" required:"false"`
	REGION     string `short:"r" long:"region" description:"AWS Region"`
	FILE       string `short:"f" long:"file"`
	DATE       string `long:"date" description:"date"`
}

var DST_BUCKET, SRC_BUCKET, FILE string

func init() {
	var opts options
	_, err := flags.ParseArgs(&opts, os.Args)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if opts.DST_BUCKET == "" {
		DST_BUCKET = os.Getenv("DST_BUCKET")
	} else {
		DST_BUCKET = opts.DST_BUCKET
	}

	if opts.SRC_BUCKET == "" {
		SRC_BUCKET = os.Getenv("SRC_BUCKET")
	} else {
		SRC_BUCKET = opts.SRC_BUCKET
	}

	if DST_BUCKET == "" || SRC_BUCKET == "" {
		log.Println("[ERROR] SRC_BUCKET and DST_BUCKET is null")
		os.Exit(1)
	}

	if opts.REGION != "" {
		os.Setenv("AWS_REGION", opts.REGION)
	}

	if os.Getenv("AWS_REGION") == "" {
		fmt.Println("[ERROR] SET the environment variable AWS_REGION")
		os.Exit(1)
	}

	FILE = opts.FILE
}

type Request struct {
	ID    int    `json:"ID"`
	Value string `json:"Value"`
}

// Response represents the Response object
type Response struct {
	Message string `json:"Message"`
	Ok      bool   `json:"Ok"`
}

// Handler represents the Handler of lambda
func Handler(request Request) (Response, error) {
	Do("aaa")
	return Response{
		Message: fmt.Sprint("Process Request Id"),
		Ok:      true,
	}, nil
}

func Do(file string) {
	log.Printf("START")
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	ctx := context.Background()
	var cancelFn func()
	ctx, cancelFn = context.WithTimeout(ctx, 120*time.Second)

	if cancelFn != nil {
		defer cancelFn()
	}

	tmpDir, err := s3.MkTmpDir("audit_")
	defer func() {
		s3.RmTmpDir(tmpDir)
		log.Printf("END")
	}()

	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	fp, err := s3.GetObject(sess, SRC_BUCKET, tmpDir, file, ctx)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	s3.ReadGzip(fp)
	s3.PutObject(sess, DST_BUCKET, fp.Name())
}

func main() {
	if os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		lambda.Start(Handler)
	} else {
		Do(FILE)
	}
}
