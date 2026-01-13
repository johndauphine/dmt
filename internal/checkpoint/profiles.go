package checkpoint

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

const (
	masterKeyEnv     = "DMT_MASTER_KEY"
	profileCipherV1  = byte(1)
	minCipherPayload = 1 + 12 // version + nonce
)

type ProfileInfo struct {
	Name        string
	Description string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// SaveProfile stores an encrypted config profile.
func (s *State) SaveProfile(name, description string, config []byte) error {
	if name == "" {
		return fmt.Errorf("profile name is required")
	}

	enc, err := encryptProfile(name, config)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(`
		INSERT INTO profiles (name, description, config_enc, created_at, updated_at)
		VALUES (?, ?, ?, datetime('now'), datetime('now'))
		ON CONFLICT(name) DO UPDATE SET
			description = excluded.description,
			config_enc = excluded.config_enc,
			updated_at = datetime('now')
	`, name, description, enc)
	return err
}

// GetProfile returns the decrypted config for a profile.
func (s *State) GetProfile(name string) ([]byte, error) {
	var enc []byte
	err := s.db.QueryRow(`SELECT config_enc FROM profiles WHERE name = ?`, name).Scan(&enc)
	if err != nil {
		return nil, err
	}
	return decryptProfile(name, enc)
}

// DeleteProfile removes a profile.
func (s *State) DeleteProfile(name string) error {
	_, err := s.db.Exec(`DELETE FROM profiles WHERE name = ?`, name)
	return err
}

// ListProfiles returns stored profile names with timestamps.
func (s *State) ListProfiles() ([]ProfileInfo, error) {
	rows, err := s.db.Query(`
		SELECT name, description, created_at, updated_at
		FROM profiles
		ORDER BY name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var profiles []ProfileInfo
	for rows.Next() {
		var p ProfileInfo
		var createdAtStr, updatedAtStr string
		var desc sql.NullString
		if err := rows.Scan(&p.Name, &desc, &createdAtStr, &updatedAtStr); err != nil {
			return nil, err
		}
		if desc.Valid {
			p.Description = desc.String
		}
		p.CreatedAt, _ = time.Parse("2006-01-02 15:04:05", createdAtStr)
		p.UpdatedAt, _ = time.Parse("2006-01-02 15:04:05", updatedAtStr)
		profiles = append(profiles, p)
	}
	return profiles, rows.Err()
}

func encryptProfile(name string, plaintext []byte) ([]byte, error) {
	key, err := getMasterKey()
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("init cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("init gcm: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("nonce: %w", err)
	}

	ciphertext := gcm.Seal(nil, nonce, plaintext, []byte(name))
	payload := append([]byte{profileCipherV1}, nonce...)
	payload = append(payload, ciphertext...)
	return payload, nil
}

func decryptProfile(name string, payload []byte) ([]byte, error) {
	if len(payload) < minCipherPayload {
		return nil, errors.New("encrypted profile payload is too short")
	}
	if payload[0] != profileCipherV1 {
		return nil, fmt.Errorf("unsupported profile cipher version: %d", payload[0])
	}

	key, err := getMasterKey()
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("init cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("init gcm: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(payload) < 1+nonceSize {
		return nil, errors.New("encrypted profile payload missing nonce")
	}
	nonce := payload[1 : 1+nonceSize]
	ciphertext := payload[1+nonceSize:]

	plaintext, err := gcm.Open(nil, nonce, ciphertext, []byte(name))
	if err != nil {
		return nil, fmt.Errorf("decrypt profile: %w", err)
	}
	return plaintext, nil
}

func getMasterKey() ([]byte, error) {
	raw := os.Getenv(masterKeyEnv)
	if raw == "" {
		return nil, fmt.Errorf("%s is not set", masterKeyEnv)
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("%s must be base64-encoded: %w", masterKeyEnv, err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("%s must decode to 32 bytes (got %d)", masterKeyEnv, len(key))
	}
	return key, nil
}
