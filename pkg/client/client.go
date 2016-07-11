package client

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"

	"github.com/legionus/kavka/pkg/api"
)

const (
	defaultServer = "127.0.0.1"
)

// normalizeHost standardizes the registry URL
func normalizeHost(name string) (*url.URL, error) {
	prefix := name
	if len(prefix) == 0 {
		prefix = defaultServer
	}

	if !strings.HasPrefix(prefix, "http://") && !strings.HasPrefix(prefix, "https://") {
		prefix = "http://" + prefix
	}

	return url.Parse(prefix)
}

// client implements the Client interface
type Client struct {
	url        url.URL
	httpClient http.Client
}

// New returns a client object which allows public access to server.
func New(hostname string, dialTimeout time.Duration) (*Client, error) {
	rt := &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 30 * time.Second,
		}).Dial,
	}

	jar, _ := cookiejar.New(nil)
	client := &http.Client{
		Jar:       jar,
		Transport: rt,
	}

	u, err := normalizeHost(hostname)
	if err != nil {
		return nil, err
	}

	return &Client{
		url:        *u,
		httpClient: *client,
	}, nil
}

func (c *Client) Ping(dgst string) (bool, error) {
	u := c.url
	u.Path = api.PingPath + "/" + dgst

	resp, err := c.httpClient.Get(u.String())
	if err != nil {
		return false, fmt.Errorf("error getting response from %s: %v", u.String(), err)
	}
	defer resp.Body.Close()

	return (resp.StatusCode == 200), nil
}

func (c *Client) GetBlob(dgst string) ([]byte, error) {
	u := c.url
	u.Path = api.BlobsPath + "/" + dgst

	resp, err := c.httpClient.Get(u.String())
	if err != nil {
		return nil, fmt.Errorf("error getting blob from %s: %v", u.String(), err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("can't read blob body from %s: %v", u.String(), err)
	}

	return body, nil
}
