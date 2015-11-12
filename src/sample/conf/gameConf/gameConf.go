package gameConf

import (
	_ "github.com/BurntSushi/toml"
)

type GameConfig struct {
	Server ServerConfig
	Db     DbConfig
	Kvs    KvsConfig
}

type ServerConfig struct {
	Host  string        `toml:"host"`
	Port  string        `toml:"port"`
	Slave []SlaveServer `toml:"slave"`
}

type SlaveServer struct {
	Weight int    `toml:"weight"`
	Host   string `toml:"host"`
	Port   string `toml:"port"`
}

type DbConfig struct {
	User  string `toml:"user"`
	Pass  string `toml:"pass"`
	Shard int    `toml:"shard"`
}

type KvsConfig struct {
	Host string `toml:"host"`
	Port string `toml:"port"`
}
