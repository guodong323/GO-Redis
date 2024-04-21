package db

import (
	"GO-Redis/Data"
	"context"
	"net"
	"strings"
)

type cmdBytes = [][]byte

var cmdTable = make(map[string]*command)

type cmdExecutor func(ctx context.Context, db *DB, cmd [][]byte, conn net.Conn) Data.RedisData
type command struct {
	Executor cmdExecutor
}

func RegisterCommand(cmdName string, executor cmdExecutor) {
	cmdTable[cmdName] = &command{
		Executor: executor,
	}
}

func MakeCommandBytes(input string) cmdBytes {
	cmdStr := strings.Split(input, " ")
	cmd := make(cmdBytes, 0)
	for _, c := range cmdStr {
		cmd = append(cmd, []byte(c))
	}
	return cmd
}
