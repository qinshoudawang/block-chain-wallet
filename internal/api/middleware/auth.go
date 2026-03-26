package middleware

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"wallet-system/internal/helpers"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v4"
)

type JWTAuthConfig struct {
	Enabled       bool
	Issuer        string
	Audience      string
	JWKSURL       string
	RequiredScope string
}

type JWTAuthMiddleware struct {
	cfg        JWTAuthConfig
	httpClient *http.Client

	mu   sync.RWMutex
	keys map[string]crypto.PublicKey
}

func LoadJWTAuthConfigFromEnv() JWTAuthConfig {
	enabled := strings.EqualFold(helpers.Getenv("API_AUTH_ENABLED", "false"), "true")
	issuer := strings.TrimRight(helpers.Getenv("API_AUTH_ISSUER", ""), "/")
	jwksURL := helpers.Getenv("API_AUTH_JWKS_URL", "")
	if jwksURL == "" && issuer != "" {
		jwksURL = issuer + "/protocol/openid-connect/certs"
	}
	return JWTAuthConfig{
		Enabled:       enabled,
		Issuer:        issuer,
		Audience:      helpers.Getenv("API_AUTH_AUDIENCE", ""),
		JWKSURL:       jwksURL,
		RequiredScope: helpers.Getenv("API_AUTH_REQUIRED_SCOPE", ""),
	}
}

func NewJWTAuthMiddlewareFromEnv() (*JWTAuthMiddleware, error) {
	return NewJWTAuthMiddleware(LoadJWTAuthConfigFromEnv())
}

func NewJWTAuthMiddleware(cfg JWTAuthConfig) (*JWTAuthMiddleware, error) {
	if !cfg.Enabled {
		return &JWTAuthMiddleware{
			cfg:        cfg,
			httpClient: &http.Client{Timeout: 5 * time.Second},
			keys:       make(map[string]crypto.PublicKey),
		}, nil
	}
	if cfg.Issuer == "" {
		return nil, errors.New("API_AUTH_ISSUER is required when API_AUTH_ENABLED=true")
	}
	if cfg.Audience == "" {
		return nil, errors.New("API_AUTH_AUDIENCE is required when API_AUTH_ENABLED=true")
	}
	if cfg.JWKSURL == "" {
		return nil, errors.New("API_AUTH_JWKS_URL is required when API_AUTH_ENABLED=true")
	}

	mw := &JWTAuthMiddleware{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		keys:       make(map[string]crypto.PublicKey),
	}
	if err := mw.refreshKeys(context.Background()); err != nil {
		return nil, fmt.Errorf("load jwks failed: %w", err)
	}
	return mw, nil
}

func (m *JWTAuthMiddleware) Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		if m == nil || !m.cfg.Enabled {
			c.Next()
			return
		}

		tokenString := bearerToken(c.GetHeader("Authorization"))
		if tokenString == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing bearer token"})
			return
		}

		claims, err := m.ValidateToken(tokenString)
		if err != nil {
			log.Printf("[api-auth] jwt validation failed path=%s err=%v", c.FullPath(), err)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}

		c.Set("auth_claims", claims)
		c.Next()
	}
}

func (m *JWTAuthMiddleware) ValidateToken(tokenString string) (jwt.MapClaims, error) {
	if m == nil {
		return nil, errors.New("auth middleware is nil")
	}
	if !m.cfg.Enabled {
		return jwt.MapClaims{}, nil
	}

	claims := jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, m.keyFunc)
	if err != nil || token == nil || !token.Valid {
		return nil, fmt.Errorf("parse jwt failed: %w", err)
	}
	if !claims.VerifyIssuer(m.cfg.Issuer, true) {
		return nil, errors.New("invalid issuer")
	}
	if !claims.VerifyAudience(m.cfg.Audience, true) {
		return nil, errors.New("invalid audience")
	}
	if m.cfg.RequiredScope != "" && !hasRequiredScope(claims, m.cfg.RequiredScope) {
		return nil, errors.New("missing required scope")
	}
	return claims, nil
}

func (m *JWTAuthMiddleware) keyFunc(token *jwt.Token) (interface{}, error) {
	if token == nil {
		return nil, errors.New("missing token")
	}
	switch token.Method.Alg() {
	case jwt.SigningMethodRS256.Alg(), jwt.SigningMethodRS384.Alg(), jwt.SigningMethodRS512.Alg(),
		jwt.SigningMethodES256.Alg(), jwt.SigningMethodES384.Alg(), jwt.SigningMethodES512.Alg():
	default:
		return nil, fmt.Errorf("unexpected signing method: %s", token.Method.Alg())
	}

	kid, _ := token.Header["kid"].(string)
	if kid == "" {
		return nil, errors.New("missing kid header")
	}

	if key, ok := m.lookupKey(kid); ok {
		return key, nil
	}
	if err := m.refreshKeys(context.Background()); err != nil {
		return nil, err
	}
	if key, ok := m.lookupKey(kid); ok {
		return key, nil
	}
	return nil, fmt.Errorf("jwks key not found for kid=%s", kid)
}

func (m *JWTAuthMiddleware) lookupKey(kid string) (crypto.PublicKey, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key, ok := m.keys[kid]
	return key, ok
}

func (m *JWTAuthMiddleware) refreshKeys(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.cfg.JWKSURL, nil)
	if err != nil {
		return err
	}
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("jwks returned status=%d", resp.StatusCode)
	}

	var payload jwksResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return err
	}

	keys := make(map[string]crypto.PublicKey, len(payload.Keys))
	for _, item := range payload.Keys {
		key, err := item.publicKey()
		if err != nil {
			return fmt.Errorf("parse jwk kid=%s failed: %w", item.Kid, err)
		}
		keys[item.Kid] = key
	}

	m.mu.Lock()
	m.keys = keys
	m.mu.Unlock()
	return nil
}

func bearerToken(header string) string {
	header = strings.TrimSpace(header)
	if header == "" {
		return ""
	}
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return strings.TrimSpace(parts[1])
}

func hasRequiredScope(claims jwt.MapClaims, scope string) bool {
	for _, value := range extractScopes(claims) {
		if value == scope {
			return true
		}
	}
	return false
}

func extractScopes(claims jwt.MapClaims) []string {
	var scopes []string
	for _, key := range []string{"scope", "scp"} {
		raw, ok := claims[key]
		if !ok {
			continue
		}
		switch v := raw.(type) {
		case string:
			scopes = append(scopes, strings.Fields(v)...)
		case []interface{}:
			for _, item := range v {
				s, ok := item.(string)
				if ok && strings.TrimSpace(s) != "" {
					scopes = append(scopes, s)
				}
			}
		}
	}
	return scopes
}

type jwksResponse struct {
	Keys []jwk `json:"keys"`
}

type jwk struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Alg string `json:"alg"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

func (k jwk) publicKey() (crypto.PublicKey, error) {
	switch k.Kty {
	case "RSA":
		return k.rsaPublicKey()
	case "EC":
		return k.ecdsaPublicKey()
	default:
		return nil, fmt.Errorf("unsupported kty=%s", k.Kty)
	}
}

func (k jwk) rsaPublicKey() (*rsa.PublicKey, error) {
	nBytes, err := decodeBase64URL(k.N)
	if err != nil {
		return nil, fmt.Errorf("decode n: %w", err)
	}
	eBytes, err := decodeBase64URL(k.E)
	if err != nil {
		return nil, fmt.Errorf("decode e: %w", err)
	}
	e := 0
	for _, b := range eBytes {
		e = e<<8 + int(b)
	}
	if e == 0 {
		return nil, errors.New("invalid rsa exponent")
	}
	return &rsa.PublicKey{
		N: new(big.Int).SetBytes(nBytes),
		E: e,
	}, nil
}

func (k jwk) ecdsaPublicKey() (*ecdsa.PublicKey, error) {
	curve, err := ellipticCurve(k.Crv)
	if err != nil {
		return nil, err
	}
	xBytes, err := decodeBase64URL(k.X)
	if err != nil {
		return nil, fmt.Errorf("decode x: %w", err)
	}
	yBytes, err := decodeBase64URL(k.Y)
	if err != nil {
		return nil, fmt.Errorf("decode y: %w", err)
	}
	return &ecdsa.PublicKey{
		Curve: curve,
		X:     new(big.Int).SetBytes(xBytes),
		Y:     new(big.Int).SetBytes(yBytes),
	}, nil
}

func ellipticCurve(name string) (elliptic.Curve, error) {
	switch name {
	case "P-256":
		return elliptic.P256(), nil
	case "P-384":
		return elliptic.P384(), nil
	case "P-521":
		return elliptic.P521(), nil
	default:
		return nil, fmt.Errorf("unsupported curve=%s", name)
	}
}

func decodeBase64URL(v string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(v)
}
