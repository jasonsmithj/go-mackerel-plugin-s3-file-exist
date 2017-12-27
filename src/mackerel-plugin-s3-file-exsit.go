package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	mp "github.com/mackerelio/go-mackerel-plugin"
	"log"
	"strings"
	"time"
)

type S3Plugin struct {
	Tempfile  string
	bucket    string
	directory string
	prefix    string
	accesskey string
	secretkey string
	region    string
	embulk    bool
}

type S3Object struct {
	count int64
	size  int64
}

func (s S3Plugin) GraphDefinition() map[string]mp.Graphs {
	labelPrefix := strings.Title(s.MetricKeyPrefix())
	return map[string]mp.Graphs{
		"": {
			Label: labelPrefix,
			Unit:  mp.UnitFloat,
			Metrics: []mp.Metrics{
				{Name: "exist", Label: "FileExist"},
			},
		},
	}
}

func (s S3Plugin) FetchMetrics() (map[string]float64, error) {
	sess := session.Must(session.NewSession())

	cli := s3.New(sess, &aws.Config{
		Credentials: credentials.NewStaticCredentials(s.accesskey, s.secretkey, ""),
		Region:      aws.String(s.region),
	})

	var fileResults float64

	if s.embulk {
		t := time.Now()
		// TODO: この条件は直したい。
		if 3 > t.Hour() {
			return map[string]float64{"exist": float64(1.0)}, nil
		}
		t = t.Add(-time.Duration(24) * time.Hour)
		const layout = "2006-01-02"
		for i := 0; i < 23; i++ {
			d := fmt.Sprintf("%.2d", i)
			ym := fmt.Sprint(t.Format(layout))
			dirname := s.directory + "/" + ym + "/" + d
			params := &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(dirname)}
			res, err := cli.ListObjectsV2(params)

			if err != nil {
				log.Fatalln("S3 Connect: ", err)
			}
			// TODO: ここの処理は関数を用意して呼出す。
			var result S3Object
			result.count = *res.KeyCount
			if result.count != 0 {
				result.size = *res.Contents[0].Size
				if result.size == 0 {
					fileResults = float64(result.count)
					break
				} else {
					fileResults = float64(result.count)
				}
			} else {
				fileResults = float64(result.count)
				break
			}
		}
	} else {
		params := &s3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(s.directory)}
		res, err := cli.ListObjectsV2(params)

		if err != nil {
			log.Fatalln("S3 Connect: ", err)
		}

		// TODO: ここの処理は関数を用意して呼出す。
		var result S3Object
		result.count = *res.KeyCount
		if result.count != 0 {
			result.size = *res.Contents[0].Size
			if result.size == 0 {
				fileResults = float64(result.count)
			} else {
				fileResults = float64(result.count)
			}
		} else {
			fileResults = float64(result.count)
		}
	}
	return map[string]float64{"exist": fileResults}, nil

}

func (s S3Plugin) MetricKeyPrefix() string {
	if s.prefix == "" {
		s.prefix = "FileExist"
	}
	return s.prefix
}

func main() {

	optMetricKeyPrefix := flag.String("metric-key-prefix", "FileExist", "File Exist")
	optTempfile := flag.String("tempfile", "", "Temp file name")
	optBucket := flag.String("bucket", "", "S3 Bucket name")
	optDirectory := flag.String("directory", "", "S3 Bucket Directory name")
	optAccessKeyId := flag.String("accesskey", "", "AWS ACCESS KEY")
	optSecretAccessKey := flag.String("secretkey", "", "AWS SECRET ACESS KEY")
	optRegion := flag.String("region", "", "AWS REGION")
	optEmbulk := flag.Bool("embulk", false, "Embulk Option")
	flag.Parse()

	var s3 S3Plugin

	s3.directory = *optDirectory
	s3.accesskey = *optAccessKeyId
	s3.secretkey = *optSecretAccessKey
	s3.prefix = *optMetricKeyPrefix
	s3.region = *optRegion
	s3.bucket = *optBucket
	s3.embulk = *optEmbulk

	helper := mp.NewMackerelPlugin(s3)
	helper.Tempfile = *optTempfile
	if helper.Tempfile == "" {
		helper.Tempfile = fmt.Sprintf("/tmp/mackerel-plugin-%s", *optMetricKeyPrefix)
	}
	helper.Run()
}
