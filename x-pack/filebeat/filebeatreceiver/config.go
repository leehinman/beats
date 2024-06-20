package filebeatreceiver

import "fmt"

// Config is the config settings in the collector's config.yaml
type Config struct {
	FakeString string `mapstructure:"fake_string"`
        Beatconfig map[string]interface{} `mapstructure:"beatconfig"`
}

// Validate checks if the configuration in valid
func (cfg *Config) Validate() error {
	if cfg.FakeString == "" {
		return fmt.Errorf("Alas, you forgot the fake_string")
	}
	return nil
}
