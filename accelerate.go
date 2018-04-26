package s3gof3r

import (
	"fmt"
	"time"
)

const defaultRegion = "us-west-2"

type S3Accelerated struct {
	region string
	*Keys
}

func NewAcceleratedS3(k *Keys) (a *S3Accelerated, err error) {
	a = &S3Accelerated{Keys: k, region: defaultRegion}
	return
}

func NewAcceleratedS3InRegion(k *Keys, region string) (a *S3Accelerated, err error) {
	a = &S3Accelerated{Keys: k, region: region}
	return
}

func (a *S3Accelerated) Region() string {
	return a.region
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
