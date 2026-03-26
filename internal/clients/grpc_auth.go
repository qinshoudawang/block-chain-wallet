package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"wallet-system/internal/helpers"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type OIDCClientCredentialsConfig struct {
	Enabled      bool
	TokenURL     string
	ClientID     string
	ClientSecret string
	Scope        string
}

func loadOIDCClientCredentialsConfig(prefix string) OIDCClientCredentialsConfig {
	prefix = strings.TrimSpace(prefix)
	return OIDCClientCredentialsConfig{
		Enabled:      strings.EqualFold(helpers.Getenv(prefix+"_OIDC_ENABLED", "false"), "true"),
		TokenURL:     helpers.Getenv(prefix+"_OIDC_TOKEN_URL", ""),
		ClientID:     helpers.Getenv(prefix+"_OIDC_CLIENT_ID", ""),
		ClientSecret: helpers.Getenv(prefix+"_OIDC_CLIENT_SECRET", ""),
		Scope:        helpers.Getenv(prefix+"_OIDC_SCOPE", ""),
	}
}

type bearerTokenSource struct {
	cfg        OIDCClientCredentialsConfig
	httpClient *http.Client

	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

func newBearerTokenSource(cfg OIDCClientCredentialsConfig) (*bearerTokenSource, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if strings.TrimSpace(cfg.TokenURL) == "" {
		return nil, errors.New("oidc token url is required when oidc auth is enabled")
	}
	if strings.TrimSpace(cfg.ClientID) == "" {
		return nil, errors.New("oidc client id is required when oidc auth is enabled")
	}
	if strings.TrimSpace(cfg.ClientSecret) == "" {
		return nil, errors.New("oidc client secret is required when oidc auth is enabled")
	}
	return &bearerTokenSource{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}, nil
}

func (s *bearerTokenSource) unaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req interface{},
		reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if s == nil {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		token, err := s.tokenForContext(ctx)
		if err != nil {
			return err
		}
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func (s *bearerTokenSource) tokenForContext(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.token != "" && time.Now().Before(s.expiresAt.Add(-30*time.Second)) {
		return s.token, nil
	}

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", s.cfg.ClientID)
	form.Set("client_secret", s.cfg.ClientSecret)
	if strings.TrimSpace(s.cfg.Scope) != "" {
		form.Set("scope", s.cfg.Scope)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.TokenURL, bytes.NewBufferString(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("oidc token endpoint returned status=%d", resp.StatusCode)
	}

	var payload tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", err
	}
	if strings.TrimSpace(payload.AccessToken) == "" {
		return "", errors.New("oidc token endpoint returned empty access token")
	}

	s.token = payload.AccessToken
	lifetime := time.Duration(payload.ExpiresIn) * time.Second
	if lifetime <= 0 {
		lifetime = 5 * time.Minute
	}
	s.expiresAt = time.Now().Add(lifetime)
	return s.token, nil
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	TokenType   string `json:"token_type"`
}
