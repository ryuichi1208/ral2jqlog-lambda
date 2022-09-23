package s3

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func MkTmpDir(prefix string) (string, error) {
	dir, err := ioutil.TempDir("", prefix)
	if err != nil {
		return "", err
	}
	return dir, nil
}

func RmTmpDir(dir string) error {
	err := os.RemoveAll(dir)
	if err != nil {
		return err
	}
	return nil
}

func GetObject(sess *session.Session, src string, tmpDir string, obj string, ctx context.Context) (*os.File, error) {
	downloader := s3manager.NewDownloader(sess)
	filename := fmt.Sprintf("%s/%s", tmpDir, filepath.Base(obj))
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	_, err = downloader.DownloadWithContext(ctx, fp, &s3.GetObjectInput{
		Bucket: aws.String(src),
		Key:    aws.String(obj),
	})
	if err != nil {
		return nil, err
	}

	return fp, nil
}

func makeHIVEFormat(filename string) string {
	arr := strings.Split(filepath.Base(filename), "-")
	hive := fmt.Sprintf("dt=%s-%s-%s-%s", arr[7], arr[8], arr[9], arr[10])
	return hive
}

func PutObject(sess *session.Session, dst string, file string) error {
	uploader := s3manager.NewUploader(sess)

	key := makeHIVEFormat(file)
	fp, err := os.Open(fmt.Sprintf("%s.json", file))
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(dst),
		Key:    aws.String(fmt.Sprintf("%s/%s", key, filepath.Base(fp.Name()))),
		Body:   fp,
	})
	if err != nil {
		return err
	}
	log.Printf("[INFO] done %s", fp.Name())

	return nil
}

type AuditLog struct {
	MessageType         string   `json:"messageType"`
	Owner               string   `json:"owner"`
	LogGroup            string   `json:"logGroup"`
	LogStream           string   `json:"logStream"`
	SubscriptionFilters []string `json:"subscriptionFilters"`
	LogEvents           []struct {
		ID        string `json:"id"`
		Timestamp int64  `json:"timestamp"`
		Message   string `json:"message"`
	} `json:"logEvents"`
}

type QueryLog struct {
	TimeStamp string `json:"timeStamp"`
	User      string `json:"user"`
	Client    string `json:"client"`
	Host      string `json:"host"`
	Command   string `json:"command"`
	Query     string `json:"query"`
}

type options struct {
	File string `short:"f" long:"file" description:"audit log file" required:"true"`
	Type string `short:"t" long:"type" description:"File Content Type" required:"false"`
}

func message2CSV(csvString string, w io.Writer) error {
	arr := strings.Split(csvString, ",")

	if len(arr) < 9 {
		return fmt.Errorf("CSV is broken")
	}

	var queryLog = QueryLog{}
	queryLog.TimeStamp = arr[0]
	queryLog.Host = arr[1]
	queryLog.User = arr[2]
	queryLog.Client = arr[3]
	queryLog.Command = arr[6]
	s := strings.Join(arr[8:len(arr)-1], ",")[1:]
	queryLog.Query = s[:len(s)-1]

	outputJson, err := json.Marshal(&queryLog)
	if err != nil {
		return err
	}

	fmt.Fprintf(w, "%s\n", string(outputJson))

	return nil
}

func auditLog2Json(jsonString string, w io.Writer) error {
	var auditlog AuditLog
	if err := json.Unmarshal([]byte(jsonString), &auditlog); err != nil {
		fmt.Println(err)
		return err
	}
	for _, v := range auditlog.LogEvents {
		if err := message2CSV(v.Message, w); err != nil {
			return err
		}
	}

	return nil
}

func ReadGzip(fp *os.File) error {
	br := bufio.NewReader(fp)
	r, err := gzip.NewReader(br)
	if err != nil {
		return err
	}
	defer r.Close()

	f, err := os.OpenFile(fmt.Sprintf("%s.json", fp.Name()), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil
	}

	for {
		r.Multistream(false)
		if data, err := ioutil.ReadAll(r); err == nil {
			auditLog2Json(string(data), f)
		}

		if err := r.Reset(br); err != nil {
			break
		}
	}
	return err
}
