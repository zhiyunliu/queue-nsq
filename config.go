package nsq

import (
	"io"
	"log"
	"strings"

	"github.com/zhiyunliu/glue/config"
	"github.com/zhiyunliu/golibs/xnet"
)

// ServerConfig holds NSQ connection settings resolved from the glue config tree.
type ServerConfig struct {
	// NsqdAddrs is the list of nsqd TCP addresses (e.g. "127.0.0.1:4150") for producer.
	NsqdAddrs []string `json:"nsqd_addrs" yaml:"nsqd_addrs"`
	// LookupdAddrs is the list of nsqlookupd HTTP addresses (e.g. "127.0.0.1:4161") for consumer discovery.
	LookupdAddrs []string `json:"lookupd_addrs" yaml:"lookupd_addrs"`
	// LogLevel 控制 go-nsq 内置日志：设为 "none" / "off" / "disabled" 时禁用 NSQ 日志输出。
	LogLevel string `json:"log_level" yaml:"log_level"`
}

// ProducerOptions holds producer-level settings.
type ProducerOptions struct {
	DelayInterval int `json:"delay_interval" yaml:"delay_interval"`
}

// ConsumerConfig holds consumer-level settings.
type ConsumerConfig struct {
	GroupName       string `json:"group_name" yaml:"group_name"`
	DeadLetterQueue string `json:"deadletter_queue" yaml:"deadletter_queue"`
}

// getNsqConfig parses the "addr" field (format: nsq://configName), resolves the
// NSQ server settings from the glue config tree, and returns server config.
func getNsqConfig(cfg config.Config) (addr string, serverCfg *ServerConfig, err error) {
	addrVal := cfg.Value("addr").String()
	protoType, configName, err := xnet.Parse(addrVal)
	if err != nil {
		return "", nil, err
	}
	rootCfg := cfg.Root()
	nsqCfg := rootCfg.Get(protoType).Get(configName)
	serverCfg = &ServerConfig{}
	_ = nsqCfg.ScanTo(serverCfg)
	return addrVal, serverCfg, nil
}

// firstNsqdAddr returns the first nsqd address for producer connection.
func firstNsqdAddr(serverCfg *ServerConfig) string {
	if len(serverCfg.NsqdAddrs) == 0 {
		return "127.0.0.1:4150"
	}
	return strings.TrimPrefix(strings.TrimPrefix(serverCfg.NsqdAddrs[0], "http://"), "https://")
}

// lookupdAddrs returns nsqlookupd addresses; if empty, uses nsqd addrs as fallback for direct connect.
func lookupdAddrs(serverCfg *ServerConfig) []string {
	if len(serverCfg.LookupdAddrs) > 0 {
		return serverCfg.LookupdAddrs
	}
	return serverCfg.NsqdAddrs
}

// shouldDisableNsqLog 判断配置是否要求禁用 NSQ 内置日志。
func shouldDisableNsqLog(logLevel string) bool {
	switch strings.ToLower(strings.TrimSpace(logLevel)) {
	case "none", "off", "disabled":
		return true
	default:
		return false
	}
}

// discardNsqLogger 用于禁用 go-nsq 日志时传入 SetLogger。
func discardNsqLogger() *log.Logger { return log.New(io.Discard, "", 0) }
