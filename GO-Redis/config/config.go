package config

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

var Configures *Config

var (
	defaultHost              = "127.0.0.1"
	defaultPort              = 6380
	defaultLogDir            = "./"
	defaultLogLevel          = "info"
	defaultSharedNumber      = 1024
	defaultChannelBufferSize = 10
)

type Config struct {
	ConfFile          string
	Host              string
	Port              int
	LogDir            string
	LogLevel          string
	ShardNumber       int
	ChannelBufferSize int
	Databases         int
	Others            map[string]any
}

type ConfError struct {
	message string
}

func (confErr *ConfError) Error() string {
	return confErr.message
}

func Init(cfg *Config) {
	flag.StringVar(&(cfg.Host), "host", defaultHost, "Bind host ip: default is 127.0.0.1")
	flag.IntVar(&(cfg.Port), "port", defaultPort, "Bind host ip: default is 127.0.0.1")
	flag.StringVar(&(cfg.LogDir), "logdir", defaultLogDir, "Create log directory: default is /tmp")
	flag.StringVar(&(cfg.LogLevel), "loglevel", defaultLogLevel, "Create log level: default is info")
	flag.IntVar(&(cfg.ChannelBufferSize), "channelbuffersize", defaultChannelBufferSize, "set the buffer size of channels in PUB/SUB commands. ")
}

func Setup() (*Config, error) {
	cfg := &Config{
		Host:              defaultHost,
		Port:              defaultPort,
		LogDir:            defaultLogDir,
		LogLevel:          defaultLogLevel,
		ShardNumber:       defaultSharedNumber,
		ChannelBufferSize: defaultChannelBufferSize,
		Databases:         16,
		Others:            make(map[string]any),
	}
	// init information
	Init(cfg)
	// parse command line flags
	flag.Parse()
	// parse config file & checks
	if cfg.ConfFile != "" {
		if err := cfg.ParseConfFile(cfg.ConfFile); err != nil {
			return nil, err
		}
	} else {
		if ip := net.ParseIP(cfg.Host); ip == nil {
			ipErr := &ConfError{
				message: fmt.Sprintf("Given ip address %s is invalid", cfg.Host),
			}
			return nil, ipErr
		}
		if cfg.Port <= 1024 || cfg.Port >= 65535 {
			portErr := &ConfError{
				message: fmt.Sprintf("Listening port should between 1024 and 65535, but %d is given.", cfg.Port),
			}
			return nil, portErr
		}
	}

	return cfg, nil
}

// parse the config file and return error
func (cfg *Config) ParseConfFile(confFile string) error {
	file, err := os.Open(confFile)
	if err != nil {
		return err
	}

	defer func() {
		err := file.Close()
		if err != nil {
			fmt.Printf("Close config file err: %s \n", err.Error())
		}
	}()

	reader := bufio.NewReader(file)
	for {
		line, ioErr := reader.ReadString('\n')
		if ioErr != nil && ioErr != io.EOF {
			return ioErr
		}

		if len(line) > 0 && line[0] == '#' {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) >= 2 {
			cfgName := strings.ToLower(fields[0])
			switch cfgName {
			case "host":
				if ip := net.ParseIP(fields[1]); ip == nil {
					ipErr := &ConfError{
						message: fmt.Sprintf("Given ip address %s is invalid", cfg.Host),
					}
					return ipErr
				}
				cfg.Host = fields[1]
			case "port":
				port, err := strconv.Atoi(fields[1])
				if err != nil {
					return err
				}
				if port <= 1024 || port >= 65535 {
					portErr := &ConfError{
						message: fmt.Sprintf("Listening port should between 1024 and 65535, but %d is given.", port),
					}
					return portErr
				}
				cfg.Port = port
			case "logdir":
				cfg.LogDir = strings.ToLower(fields[1])
			case "loglevel":
				cfg.LogLevel = strings.ToLower(fields[1])
			case "sharedNumber":
				cfg.ShardNumber, err = strconv.Atoi(fields[1])
				if err != nil {
					fmt.Println("ShardNum should be a number. Get: ", fields[1])
					panic(err)
				}
			case "databases":
				cfg.ShardNumber, err = strconv.Atoi(fields[1])
				if err != nil {
					log.Fatal("Databases should be an integer. Get: ", fields[1])
				}
				if cfg.Databases <= 0 {
					log.Fatal("Databases should be an positive integer. Get: ", fields[1])
				}
			default:
				cfg.Others[cfgName] = fields[1]
			}
		}
		if ioErr == io.EOF {
			break
		}
	}
	return nil
}
