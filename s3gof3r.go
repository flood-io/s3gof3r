// Package s3gof3r provides fast, parallelized, streaming access to Amazon S3. It includes a command-line interface: `gof3r`.
package s3gof3r

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
)

const versionParam = "versionId"

var regionMatcher = regexp.MustCompile("s3[-.]([a-z0-9-]+).amazonaws.com([.a-z0-9]*)")

// S3 contains the domain or endpoint of an S3-compatible service and
// the authentication keys for that service.
type S3 struct {
	domain string // The s3-compatible endpoint. Defaults to "s3.amazonaws.com"
	*Keys
}

// Region returns the service region infering it from S3 domain.
func (s *S3) Region() string {
	region := os.Getenv("AWS_REGION")
	switch s.Domain() {
	case "s3.amazonaws.com", "s3-external-1.amazonaws.com":
		return "us-east-1"
	case "s3-accelerate.amazonaws.com":
		if region == "" {
			panic("can't find endpoint region")
		}
		return region
	default:
		regions := regionMatcher.FindStringSubmatch(s.Domain())
		if len(regions) < 2 {
			if region == "" {
				panic("can't find endpoint region")
			}
			return region
		}
		return regions[1]
	}
}

func (s *S3) Domain() string {
	return s.domain
}

func (s *S3) DomainForBucket(bucket string) string {
	return fmt.Sprintf("%s.%s", bucket, s.Domain())
}

// DefaultDomain is set to the endpoint for the U.S. S3 service.
const DefaultDomain = "s3.amazonaws.com"

// New Returns a new S3
// domain defaults to DefaultDomain if empty
func New(domain string, keys *Keys) *S3 {
	if domain == "" {
		domain = DefaultDomain
	}
	return &S3{domain: domain, Keys: keys}
}

// DefaultConfig contains defaults used if *Config is nil
var DefaultConfig = &Config{
	Concurrency: 10,
	PartSize:    20 * mb,
	NTry:        10,
	Md5Check:    true,
	Scheme:      "https",
	Client:      ClientWithTimeout(defaultClientTimeout),
}

// Bucket returns a bucket on s3
// Bucket Config is initialized to DefaultConfig
func (s *S3) Bucket(name string) *Bucket {
	bucket, _ := NewBucket(s, name, DefaultConfig)
	return bucket
}

// SetLogger wraps the standard library log package.
//
// It allows the internal logging of s3gof3r to be set to a desired output and format.
// Setting debug to true enables debug logging output. s3gof3r does not log output by default.
func SetLogger(out io.Writer, prefix string, flag int, debug bool) {
	logger = internalLogger{
		log.New(out, prefix, flag),
		debug,
	}
}

type internalLogger struct {
	*log.Logger
	debug bool
}

var logger internalLogger

func (l *internalLogger) debugPrintln(v ...interface{}) {
	if logger.debug {
		logger.Println(v...)
	}
}

func (l *internalLogger) debugPrintf(format string, v ...interface{}) {
	if logger.debug {
		logger.Printf(format, v...)
	}
}

// Initialize internal logger to log to no-op (ioutil.Discard) by default.
func init() {
	logger = internalLogger{
		log.New(ioutil.Discard, "", log.LstdFlags),
		false,
	}
}
