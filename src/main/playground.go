package main

import (
	"6.5840/shardctrler"
	"github.com/google/go-cmp/cmp"
)

func main() {
	a := shardctrler.NewConfigData{}
	b := shardctrler.NewConfigData{}
	print(cmp.Equal(a, b))
}
