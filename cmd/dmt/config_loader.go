package main

import (
	"fmt"
	"os"

	"github.com/johndauphine/dmt/v5/internal/checkpoint"
	"github.com/johndauphine/dmt/v5/internal/config"

	"github.com/urfave/cli/v2"
)

func getConfigPath(c *cli.Context) (path string, isSet bool) {
	for _, ctx := range c.Lineage() {
		if ctx != nil && ctx.IsSet("config") {
			return ctx.String("config"), true
		}
	}
	return "config.yaml", false
}

func loadConfigWithOrigin(c *cli.Context) (*config.Config, string, string, error) {
	profileName := c.String("profile")
	if profileName != "" {
		cfg, err := loadProfileConfig(profileName)
		return cfg, profileName, "", err
	}

	configPath, configIsSet := getConfigPath(c)
	if _, err := os.Stat(configPath); os.IsNotExist(err) && !configIsSet {
		return nil, "", "", fmt.Errorf("configuration file not found: %s", configPath)
	}
	cfg, err := config.Load(configPath)
	return cfg, "", configPath, err
}

func loadProfileConfig(name string) (*config.Config, error) {
	dataDir, err := config.DefaultDataDir()
	if err != nil {
		return nil, err
	}
	state, err := checkpoint.New(dataDir)
	if err != nil {
		return nil, err
	}
	defer state.Close()

	blob, err := state.GetProfile(name)
	if err != nil {
		return nil, err
	}
	return config.LoadBytes(blob)
}
