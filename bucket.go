package s3gof3r

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

type S3ConfigSource interface {
	Domain() string
	DomainForBucket(bucket string) string
	Region() string

	AccessKeyID() string
	SecretAccessKey() string
	SessionToken() string
}

// Config includes configuration parameters for s3gof3r
type Config struct {
	Client      *http.Client // http client to use for requests
	Concurrency int          // number of parts to get or put concurrently
	PartSize    int64        // initial  part size in bytes to use for multipart gets or puts
	NTry        int          // maximum attempts for each part
	Md5Check    bool         // The md5 hash of the object is stored in <bucket>/.md5/<object_key>.md5
	// When true, it is stored on puts and verified on gets
	Scheme    string // url scheme, defaults to 'https'
	PathStyle bool   // use path style bucket addressing instead of virtual host style
}

// A Bucket for an S3 service.
type Bucket struct {
	S3     S3ConfigSource
	Name   string
	Config *Config
}

func NewBucket(s3 S3ConfigSource, name string, config *Config) (bucket *Bucket, err error) {
	bucket = &Bucket{
		S3:     s3,
		Name:   name,
		Config: config,
	}

	return
}

// Do conveniently proxies through to the configured http client.
func (b *Bucket) Do(req *http.Request) (*http.Response, error) {
	return b.Config.Client.Do(req)
}

// GetReader provides a reader and downloads data using parallel ranged get requests.
// Data from the requests are ordered and written sequentially.
//
// Data integrity is verified via the option specified in c.
// Header data from the downloaded object is also returned, useful for reading object metadata.
// DefaultConfig is used if c is nil
// Callers should call Close on r to ensure that all resources are released.
//
// To specify an object version in a versioned bucket, the version ID may be included in the path as a url parameter. See http://docs.aws.amazon.com/AmazonS3/latest/dev/RetrievingObjectVersions.html
func (b *Bucket) GetReader(path string) (r io.ReadCloser, h http.Header, err error) {
	if path == "" {
		return nil, nil, errors.New("empty path requested")
	}
	u, err := b.url(path)
	if err != nil {
		return nil, nil, err
	}
	return newGetter(*u, b)
}

// PutWriter provides a writer to upload data as multipart upload requests.
//
// Each header in h is added to the HTTP request header. This is useful for specifying
// options such as server-side encryption in metadata as well as custom user metadata.
// Callers should call Close on w to ensure that all resources are released.
func (b *Bucket) PutWriter(path string, h http.Header) (w io.WriteCloser, err error) {
	u, err := b.url(path)
	if err != nil {
		return nil, err
	}

	return newPutter(*u, h, b)
}

// url returns a parsed url to the given path. c must not be nil
func (b *Bucket) url(bPath string) (*url.URL, error) {

	// parse versionID parameter from path, if included
	// See https://github.com/rlmcpherson/s3gof3r/issues/84 for rationale
	purl, err := url.Parse(bPath)
	if err != nil {
		return nil, err
	}
	var vals url.Values
	if v := purl.Query().Get(versionParam); v != "" {
		vals = make(url.Values)
		vals.Add(versionParam, v)
		bPath = strings.Split(bPath, "?")[0] // remove versionID from path
	}

	// handling for bucket names containing periods / explicit PathStyle addressing
	// http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html for details
	if strings.Contains(b.Name, ".") || b.Config.PathStyle {
		return &url.URL{
			Host:     b.S3.Domain(),
			Scheme:   b.Config.Scheme,
			Path:     path.Clean(fmt.Sprintf("/%s/%s", b.Name, bPath)),
			RawQuery: vals.Encode(),
		}, nil
	} else {
		return &url.URL{
			Scheme:   b.Config.Scheme,
			Path:     path.Clean(fmt.Sprintf("/%s", bPath)),
			Host:     path.Clean(b.S3.DomainForBucket(b.Name)),
			RawQuery: vals.Encode(),
		}, nil
	}
}

// Delete deletes the key at path
// If the path does not exist, Delete returns nil (no error).
func (b *Bucket) Delete(path string) error {
	if err := b.delete(path); err != nil {
		return err
	}
	// try to delete md5 file
	if b.Config.Md5Check {
		if err := b.delete(fmt.Sprintf("/.md5/%s.md5", path)); err != nil {
			return err
		}
	}

	logger.Printf("%s deleted from %s\n", path, b.Name)
	return nil
}

func (b *Bucket) delete(path string) error {
	u, err := b.url(path)
	if err != nil {
		return err
	}
	r := http.Request{
		Method: "DELETE",
		URL:    u,
	}
	b.Sign(&r)
	resp, err := b.Do(&r)
	if err != nil {
		return err
	}
	defer checkClose(resp.Body, err)
	if resp.StatusCode != 204 {
		return newRespError(resp)
	}
	return nil
}

// ListObjects returns a list of objects under the given prefixes using parallel
// requests for each prefix and any continuations.
//
// maxKeys indicates how many keys should be returned per request
func (b *Bucket) ListObjects(prefixes []string, maxKeys int) (*ObjectLister, error) {
	return newObjectLister(b.Config, b, prefixes, maxKeys)
}

// DeleteMultiple deletes multiple keys in a single request.
//
// If 'quiet' is false, the result includes the requested paths and whether they
// were deleted.
func (b *Bucket) DeleteMultiple(quiet bool, keys ...string) (DeleteResult, error) {
	// We also want to try to delete the corresponding md5 files
	if b.Config.Md5Check {
		md5Keys := make([]string, 0, len(keys))
		for _, key := range keys {
			md5Keys = append(md5Keys, fmt.Sprintf("/.md5/%s.md", key))
		}
		keys = append(keys, md5Keys...)
	}

	return deleteMultiple(b, quiet, keys)
}

// Sign signs the http.Request
func (b *Bucket) Sign(req *http.Request) {
	if req.Header == nil {
		req.Header = http.Header{}
	}
	req.Header.Set("User-Agent", "S3Gof3r")
	s := &signer{
		Time:     time.Now(),
		Request:  req,
		S3Config: b.S3,
	}
	s.sign()
}
