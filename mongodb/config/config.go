package config

type MongoConfig struct {
	Enable  bool   `mapstructure:"enable" json:"enable" yaml:"enable"`    // 开启状态
	URL     string `mapstructure:"url" json:"url" yaml:"url"`             // 链接地址
	Timeout int    `mapstructure:"timeout" json:"timeout" yaml:"timeout"` // 超时时间
}
