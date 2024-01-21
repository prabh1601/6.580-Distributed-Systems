package utils

import "os"

func PrintIfEnabled(envVar, msg string) {
	ExecuteIfEnabled(envVar, func() {
		println(msg)
	})
}

func ExecuteIfEnabled(envVar string, f func()) {
	if os.Getenv(envVar) == "true" {
		f()
	}
}
