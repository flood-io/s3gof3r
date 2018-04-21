package s3gof3r

import (
	"fmt"
	"time"
)

type S3Accelerated struct {
	*Keys
}

func NewAcceleratedS3(k *Keys) (a *S3Accelerated, err error) {
	a = &S3Accelerated{Keys: k}
	return
}

func (a *S3Accelerated) Region() string {
	return "us-west-2"
}

func (a *S3Accelerated) Domain() string {
	return "s3-accelerate.amazonaws.com"
}

func (a *S3Accelerated) DomainForBucket(bucket string) string {
	return fmt.Sprintf("%s.%s", bucket, a.Domain())
}

const defaultClientTimeout = 5 * time.Second

func (a *S3Accelerated) BucketWithDefaultConfig(name string) (b *Bucket) {
	config := &Config{
		Concurrency: 10,
		PartSize:    20 * mb,
		NTry:        10,
		Md5Check:    true,
		Scheme:      "https",
		Client:      ClientWithTimeout(defaultClientTimeout),
	}

	b, _ = NewBucket(a, name, config)

	return
}
