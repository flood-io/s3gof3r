package s3gof3r

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

// Keys for an Amazon Web Services account.
// Used for signing http requests.
type Keys struct {
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
}

func (k *Keys) AccessKeyID() string     { return k.accessKeyID }
func (k *Keys) SecretAccessKey() string { return k.secretAccessKey }
func (k *Keys) SessionToken() string    { return k.sessionToken }

type mdCreds struct {
	Code            string
	LastUpdated     string
	Type            string
	AccessKeyID     string `xml:"AccessKeyId"`
	SecretAccessKey string
	Token           string
	Expiration      string
}

// InstanceKeys Requests the AWS keys from the instance-based metadata on EC2
// Assumes only one IAM role.
func InstanceKeys() (keys *Keys, err error) {

	rolePath := "http://169.254.169.254/latest/meta-data/iam/security-credentials/"
	var creds mdCreds

	// request the role name for the instance
	// assumes there is only one
	resp, err := ClientWithTimeout(2 * time.Second).Get(rolePath)
	if err != nil {
		return
	}
	defer checkClose(resp.Body, err)
	if resp.StatusCode != 200 {
		err = newRespError(resp)
		return
	}
	role, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return
	}

	// request the credential metadata for the role
	resp, err = http.Get(rolePath + string(role))
	if err != nil {
		return
	}
	defer checkClose(resp.Body, err)
	if resp.StatusCode != 200 {
		err = newRespError(resp)
		return
	}
	metadata, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	if err = json.Unmarshal([]byte(metadata), &creds); err != nil {
		return
	}
	keys = &Keys{
		accessKeyID:     creds.AccessKeyID,
		secretAccessKey: creds.SecretAccessKey,
		sessionToken:    creds.Token,
	}

	return
}

// EnvKeys Reads the AWS keys from the environment
func EnvKeys() (keys *Keys, err error) {
	keys = &Keys{
		accessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		secretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		sessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
	}
	if keys.accessKeyID == "" || keys.secretAccessKey == "" {
		err = fmt.Errorf("keys not set in environment: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY")
	}
	return
}
