package btc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Config struct {
	Host       string
	DisableTLS bool
	Params     string
}

type Client struct {
	httpClient *http.Client
	httpBase   string
}

type UTXO struct {
	TxID          string
	Vout          uint32
	ValueSat      int64
	Confirmations int64
}

func NewClient(cfg Config) (*Client, error) {
	host := strings.TrimSpace(cfg.Host)
	if host == "" {
		return nil, errors.New("btc api host is required")
	}

	baseURL := host
	if !strings.Contains(baseURL, "://") {
		scheme := "https"
		if cfg.DisableTLS {
			scheme = "http"
		}
		baseURL = scheme + "://" + baseURL
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, errors.New("invalid btc api host")
	}
	if u.Host == "" {
		return nil, errors.New("invalid btc api host")
	}
	if u.Scheme == "" {
		u.Scheme = "https"
	}
	if u.Path == "" {
		u.Path = "/"
	}
	return &Client{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				Proxy:               http.ProxyFromEnvironment,
				DialContext:         (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
				TLSHandshakeTimeout: 10 * time.Second,
				ForceAttemptHTTP2:   false,
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		httpBase: strings.TrimRight(u.String(), "/"),
	}, nil
}

func (c *Client) Close() {
}

func (c *Client) ensureHTTP() error {
	if c == nil || c.httpClient == nil || c.httpBase == "" {
		return errors.New("btc client is required")
	}
	return nil
}

func (c *Client) httpRequest(ctx context.Context, method string, path string, contentType string, body io.Reader) ([]byte, error) {
	if err := c.ensureHTTP(); err != nil {
		return nil, err
	}

	var payload []byte
	if body != nil {
		b, err := io.ReadAll(body)
		if err != nil {
			return nil, err
		}
		payload = b
	}

	var lastErr error
	const maxRetries = 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, c.httpBase+path, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			if !isRetryableHTTPError(err) || attempt == maxRetries-1 {
				return nil, err
			}
			time.Sleep(time.Duration(attempt+1) * 300 * time.Millisecond)
			continue
		}
		raw, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			if attempt == maxRetries-1 {
				return nil, err
			}
			time.Sleep(time.Duration(attempt+1) * 300 * time.Millisecond)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			msg := strings.TrimSpace(string(raw))
			if msg == "" {
				msg = resp.Status
			}
			lastErr = fmt.Errorf("btc http api status=%d body=%s", resp.StatusCode, msg)
			if (resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500) && attempt < maxRetries-1 {
				time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
				continue
			}
			return nil, fmt.Errorf("btc http api error: %s", msg)
		}
		return raw, nil
	}
	return nil, lastErr
}

func isRetryableHTTPError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "eof") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "temporarily unavailable")
}
