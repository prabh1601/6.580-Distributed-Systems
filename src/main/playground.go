package main

import (
	"6.5840/utils"
)

func main() {
	e1 := utils.LogEntry{LogTerm: 3, LogIndex: 4, LogCommand: 4}
	e2 := utils.LogEntry{LogTerm: 3, LogIndex: 4, LogCommand: 4}

	println(e1 == e2)
}
