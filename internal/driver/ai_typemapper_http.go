package driver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/johndauphine/dmt/internal/logging"
)

// Retry configuration constants
const (
	// defaultMaxRetries is the default number of retry attempts for transient failures.
	defaultMaxRetries = 3

	// defaultBaseDelay is the initial delay between retries.
	defaultBaseDelay = 1 * time.Second

	// defaultMaxDelay is the maximum delay between retries (cap for exponential backoff).
	defaultMaxDelay = 10 * time.Second
)

// sanitizeErrorResponse truncates and sanitizes API error response bodies.
func sanitizeErrorResponse(body []byte, maxLen int) string {
	if maxLen <= 0 {
		maxLen = 200
	}

	s := string(body)
	if len(s) > maxLen {
		s = s[:maxLen] + "..."
	}

	keyPatterns := []string{"sk-", "api-", "key-", "secret-", "token-"}
	for _, pattern := range keyPatterns {
		for {
			idx := strings.Index(strings.ToLower(s), pattern)
			if idx == -1 {
				break
			}
			endIdx := idx + len(pattern) + 40
			if endIdx > len(s) {
				endIdx = len(s)
			}
			s = s[:idx] + "[REDACTED]" + s[endIdx:]
		}
	}

	return s
}

// isRetryableError determines if an error is transient and should be retried.
// Returns true for network timeouts, temporary network errors, connection errors,
// server errors (5xx), and rate limiting responses (429).
func isRetryableError(err error, statusCode int) bool {
	// Check for retryable HTTP status codes (server errors, rate limiting)
	if statusCode >= 500 || statusCode == 429 {
		return true
	}

	if err == nil {
		return false
	}

	// Check for context deadline exceeded (timeout)
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Check for EOF errors
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// Check for network errors (timeout or temporary)
	var netErr net.Error
	if errors.As(err, &netErr) {
		// Retry on timeout or temporary network errors
		//nolint:staticcheck // Temporary() is deprecated but still useful for some net errors
		return netErr.Timeout() || netErr.Temporary()
	}

	// Check for connection errors - only retry on temporary or dial errors
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		// Only retry dial errors (connection refused, etc.) or temporary errors
		//nolint:staticcheck // Temporary() is deprecated but still useful for some net errors
		if opErr.Op == "dial" || opErr.Temporary() {
			return true
		}
		return false
	}

	// Check for common retryable error messages (fallback for wrapped errors)
	errMsg := strings.ToLower(err.Error())
	retryablePatterns := []string{
		"tls handshake timeout",
		"connection reset",
		"connection refused",
		"broken pipe",
		"no such host",
		"temporary failure",
		"i/o timeout",
		"unexpected eof", // Fallback for wrapped EOF errors
	}
	for _, pattern := range retryablePatterns {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}

	return false
}

// retryableHTTPDo executes an HTTP request with exponential backoff retry logic.
// It retries on transient network errors and server errors (5xx, 429).
func (m *AITypeMapper) retryableHTTPDo(ctx context.Context, reqFunc func() (*http.Request, error)) (*http.Response, []byte, error) {
	var lastErr error
	var lastStatusCode int

	for attempt := 0; attempt <= defaultMaxRetries; attempt++ {
		// Check context before each attempt
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}

		// Create fresh request for each attempt
		req, err := reqFunc()
		if err != nil {
			return nil, nil, fmt.Errorf("creating request: %w", err)
		}

		// Execute request
		resp, err := m.client.Do(req)
		if err != nil {
			lastErr = err
			lastStatusCode = 0

			if !isRetryableError(err, 0) {
				return nil, nil, fmt.Errorf("API request failed: %w", err)
			}

			// Log retry attempt
			if attempt < defaultMaxRetries {
				delay := calculateBackoff(attempt)
				logging.Debug("AI API request failed (attempt %d/%d): %v, retrying in %v",
					attempt+1, defaultMaxRetries+1, err, delay)

				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
			continue
		}

		// Read response body
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()

		if readErr != nil {
			lastErr = readErr

			// Only retry if the read error is retryable
			if !isRetryableError(readErr, 0) {
				return nil, nil, fmt.Errorf("reading response body: %w", readErr)
			}

			if attempt < defaultMaxRetries {
				delay := calculateBackoff(attempt)
				logging.Debug("AI API response read failed (attempt %d/%d): %v, retrying in %v",
					attempt+1, defaultMaxRetries+1, readErr, delay)

				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
			continue
		}

		lastStatusCode = resp.StatusCode

		// Check for retryable status codes
		if isRetryableError(nil, resp.StatusCode) {
			lastErr = fmt.Errorf("API returned status %d", resp.StatusCode)

			if attempt < defaultMaxRetries {
				delay := calculateBackoff(attempt)
				logging.Debug("AI API returned status %d (attempt %d/%d), retrying in %v",
					resp.StatusCode, attempt+1, defaultMaxRetries+1, delay)

				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
			continue
		}

		// Success or non-retryable error
		return resp, body, nil
	}

	// All retries exhausted
	if lastErr != nil {
		return nil, nil, fmt.Errorf("API request failed after %d attempts: %w", defaultMaxRetries+1, lastErr)
	}
	return nil, nil, fmt.Errorf("API request failed after %d attempts (status %d)", defaultMaxRetries+1, lastStatusCode)
}

// calculateBackoff returns the delay for a given retry attempt using exponential backoff with jitter.
func calculateBackoff(attempt int) time.Duration {
	// Exponential backoff: baseDelay * 2^attempt
	delay := defaultBaseDelay * time.Duration(1<<attempt)

	// Cap at max delay
	if delay > defaultMaxDelay {
		delay = defaultMaxDelay
	}

	// Add jitter (±25% randomization to prevent thundering herd)
	jitter := time.Duration(rand.Int63n(int64(delay) / 2))
	delay = delay - delay/4 + jitter

	return delay
}
