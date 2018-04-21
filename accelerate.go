package s3gof3r

import "fmt"

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
