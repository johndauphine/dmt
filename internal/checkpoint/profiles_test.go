package checkpoint

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

// testMasterKey is a valid 32-byte key for testing (base64 encoded)
var testMasterKey = base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))

func setupTestMasterKey(t *testing.T) func() {
	t.Helper()
	oldKey := os.Getenv(masterKeyEnv)
	os.Setenv(masterKeyEnv, testMasterKey)
	return func() {
		if oldKey != "" {
			os.Setenv(masterKeyEnv, oldKey)
		} else {
			os.Unsetenv(masterKeyEnv)
		}
	}
}

func TestEncryptDecryptProfile(t *testing.T) {
	cleanup := setupTestMasterKey(t)
	defer cleanup()

	tests := []struct {
		name      string
		plaintext string
	}{
		{
			name:      "simple config",
			plaintext: "source:\n  host: localhost\n",
		},
		{
			name:      "empty config",
			plaintext: "",
		},
		{
			name:      "config with special characters",
			plaintext: "password: \"p@ss!w0rd#$%\"\n",
		},
		{
			name:      "large config",
			plaintext: string(make([]byte, 10000)), // 10KB of null bytes
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			profileName := "test-profile"
			plaintext := []byte(tt.plaintext)

			// Encrypt
			encrypted, err := encryptProfile(profileName, plaintext)
			if err != nil {
				t.Fatalf("encryptProfile() error: %v", err)
			}

			// Encrypted should be different from plaintext (unless empty)
			if len(plaintext) > 0 && string(encrypted) == tt.plaintext {
				t.Error("encrypted data equals plaintext")
			}

			// Decrypt
			decrypted, err := decryptProfile(profileName, encrypted)
			if err != nil {
				t.Fatalf("decryptProfile() error: %v", err)
			}

			// Verify round-trip
			if string(decrypted) != tt.plaintext {
				t.Errorf("decrypted = %q, want %q", string(decrypted), tt.plaintext)
			}
		})
	}
}

func TestEncryptProfileDifferentNonce(t *testing.T) {
	cleanup := setupTestMasterKey(t)
	defer cleanup()

	plaintext := []byte("same config data")
	profileName := "test"

	// Encrypt same data twice
	enc1, err := encryptProfile(profileName, plaintext)
	if err != nil {
		t.Fatalf("first encryptProfile() error: %v", err)
	}

	enc2, err := encryptProfile(profileName, plaintext)
	if err != nil {
		t.Fatalf("second encryptProfile() error: %v", err)
	}

	// Should produce different ciphertext due to random nonce
	if string(enc1) == string(enc2) {
		t.Error("encrypting same data twice produced identical ciphertext")
	}

	// But both should decrypt to same plaintext
	dec1, _ := decryptProfile(profileName, enc1)
	dec2, _ := decryptProfile(profileName, enc2)

	if string(dec1) != string(dec2) {
		t.Error("decrypted values differ")
	}
}

func TestDecryptWrongProfileName(t *testing.T) {
	cleanup := setupTestMasterKey(t)
	defer cleanup()

	plaintext := []byte("secret config")

	// Encrypt with one profile name
	encrypted, err := encryptProfile("profile-a", plaintext)
	if err != nil {
		t.Fatalf("encryptProfile() error: %v", err)
	}

	// Try to decrypt with different profile name (should fail - AAD mismatch)
	_, err = decryptProfile("profile-b", encrypted)
	if err == nil {
		t.Error("expected decryption to fail with wrong profile name")
	}
}

func TestDecryptInvalidPayload(t *testing.T) {
	cleanup := setupTestMasterKey(t)
	defer cleanup()

	tests := []struct {
		name    string
		payload []byte
		wantErr string
	}{
		{
			name:    "empty payload",
			payload: []byte{},
			wantErr: "too short",
		},
		{
			name:    "payload too short",
			payload: []byte{profileCipherV1, 1, 2, 3},
			wantErr: "missing nonce",
		},
		{
			name:    "wrong version",
			payload: []byte{99, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
			wantErr: "unsupported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decryptProfile("test", tt.payload)
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

func TestGetMasterKey(t *testing.T) {
	t.Run("missing key", func(t *testing.T) {
		oldKey := os.Getenv(masterKeyEnv)
		os.Unsetenv(masterKeyEnv)
		defer func() {
			if oldKey != "" {
				os.Setenv(masterKeyEnv, oldKey)
			}
		}()

		_, err := getMasterKey()
		if err == nil {
			t.Error("expected error for missing key")
		}
	})

	t.Run("invalid base64", func(t *testing.T) {
		oldKey := os.Getenv(masterKeyEnv)
		os.Setenv(masterKeyEnv, "not-valid-base64!!!")
		defer func() {
			if oldKey != "" {
				os.Setenv(masterKeyEnv, oldKey)
			} else {
				os.Unsetenv(masterKeyEnv)
			}
		}()

		_, err := getMasterKey()
		if err == nil {
			t.Error("expected error for invalid base64")
		}
	})

	t.Run("wrong key length", func(t *testing.T) {
		oldKey := os.Getenv(masterKeyEnv)
		shortKey := base64.StdEncoding.EncodeToString([]byte("tooshort"))
		os.Setenv(masterKeyEnv, shortKey)
		defer func() {
			if oldKey != "" {
				os.Setenv(masterKeyEnv, oldKey)
			} else {
				os.Unsetenv(masterKeyEnv)
			}
		}()

		_, err := getMasterKey()
		if err == nil {
			t.Error("expected error for wrong key length")
		}
	})

	t.Run("valid key", func(t *testing.T) {
		cleanup := setupTestMasterKey(t)
		defer cleanup()

		key, err := getMasterKey()
		if err != nil {
			t.Fatalf("getMasterKey() error: %v", err)
		}

		if len(key) != 32 {
			t.Errorf("key length = %d, want 32", len(key))
		}
	})
}

func TestProfileCRUD(t *testing.T) {
	cleanup := setupTestMasterKey(t)
	defer cleanup()

	// Create temp directory for SQLite database
	tmpDir := t.TempDir()

	// Create state manager
	state, err := New(tmpDir)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	t.Run("save and get profile", func(t *testing.T) {
		configData := []byte("source:\n  host: localhost\n")

		err := state.SaveProfile("test-profile", "Test description", configData)
		if err != nil {
			t.Fatalf("SaveProfile() error: %v", err)
		}

		// Get the profile back
		retrieved, err := state.GetProfile("test-profile")
		if err != nil {
			t.Fatalf("GetProfile() error: %v", err)
		}

		if string(retrieved) != string(configData) {
			t.Errorf("retrieved = %q, want %q", string(retrieved), string(configData))
		}
	})

	t.Run("update existing profile", func(t *testing.T) {
		originalData := []byte("original: true\n")
		updatedData := []byte("updated: true\n")

		// Save original
		err := state.SaveProfile("update-test", "Original", originalData)
		if err != nil {
			t.Fatalf("SaveProfile() error: %v", err)
		}

		// Update with new data
		err = state.SaveProfile("update-test", "Updated description", updatedData)
		if err != nil {
			t.Fatalf("SaveProfile() update error: %v", err)
		}

		// Verify update
		retrieved, err := state.GetProfile("update-test")
		if err != nil {
			t.Fatalf("GetProfile() error: %v", err)
		}

		if string(retrieved) != string(updatedData) {
			t.Errorf("retrieved = %q, want %q", string(retrieved), string(updatedData))
		}
	})

	t.Run("list profiles", func(t *testing.T) {
		// Clear and add fresh profiles
		state.DeleteProfile("list-1")
		state.DeleteProfile("list-2")

		state.SaveProfile("list-1", "First profile", []byte("config1"))
		state.SaveProfile("list-2", "Second profile", []byte("config2"))

		profiles, err := state.ListProfiles()
		if err != nil {
			t.Fatalf("ListProfiles() error: %v", err)
		}

		// Should have at least 2 profiles
		if len(profiles) < 2 {
			t.Errorf("len(profiles) = %d, want >= 2", len(profiles))
		}

		// Check that our profiles are in the list
		found := 0
		for _, p := range profiles {
			if p.Name == "list-1" || p.Name == "list-2" {
				found++
			}
		}
		if found != 2 {
			t.Errorf("found %d of our test profiles, want 2", found)
		}
	})

	t.Run("delete profile", func(t *testing.T) {
		// Create a profile to delete
		err := state.SaveProfile("to-delete", "Will be deleted", []byte("config"))
		if err != nil {
			t.Fatalf("SaveProfile() error: %v", err)
		}

		// Delete it
		err = state.DeleteProfile("to-delete")
		if err != nil {
			t.Fatalf("DeleteProfile() error: %v", err)
		}

		// Try to get it (should fail)
		_, err = state.GetProfile("to-delete")
		if err == nil {
			t.Error("expected error when getting deleted profile")
		}
	})

	t.Run("get nonexistent profile", func(t *testing.T) {
		_, err := state.GetProfile("does-not-exist")
		if err == nil {
			t.Error("expected error for nonexistent profile")
		}
	})

	t.Run("save profile without name", func(t *testing.T) {
		err := state.SaveProfile("", "No name", []byte("config"))
		if err == nil {
			t.Error("expected error for empty profile name")
		}
	})
}

func TestProfileInfoFields(t *testing.T) {
	cleanup := setupTestMasterKey(t)
	defer cleanup()

	tmpDir := t.TempDir()
	state, err := New(tmpDir)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	// Save a profile with description
	err = state.SaveProfile("info-test", "This is a test profile", []byte("config"))
	if err != nil {
		t.Fatalf("SaveProfile() error: %v", err)
	}

	profiles, err := state.ListProfiles()
	if err != nil {
		t.Fatalf("ListProfiles() error: %v", err)
	}

	var found *ProfileInfo
	for i := range profiles {
		if profiles[i].Name == "info-test" {
			found = &profiles[i]
			break
		}
	}

	if found == nil {
		t.Fatal("profile not found in list")
	}

	if found.Description != "This is a test profile" {
		t.Errorf("Description = %q, want %q", found.Description, "This is a test profile")
	}

	if found.CreatedAt.IsZero() {
		t.Error("CreatedAt is zero")
	}

	if found.UpdatedAt.IsZero() {
		t.Error("UpdatedAt is zero")
	}
}

func TestDatabasePath(t *testing.T) {
	tmpDir := t.TempDir()

	state, err := New(tmpDir)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer state.Close()

	// Verify database file was created (named "migrate.db")
	dbPath := filepath.Join(tmpDir, "migrate.db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("database file not created at %s", dbPath)
	}
}
