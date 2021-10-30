package raft

import (
	"fmt"
	"log"
	"runtime"
	"strings"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func LPrintf(idx int, format string, a ...interface{}) (n int, err error) {
	pc, _, _, _ := runtime.Caller(1)
	f := runtime.FuncForPC(pc)
	func_name := f.Name()
	func_name_parts := strings.Split(func_name, "/")
	func_name = func_name_parts[len(func_name_parts)-1]
	file, line := f.FileLine(pc)
	perfix := fmt.Sprintf("[%s:%d %s] Server-%d: ", file, line, func_name, idx)
	perfix += format
	if Debug > 0 {
		log.Printf(perfix, a...)
	}
	return
}

func assert(condition bool) {
	if !condition {
		panic("Assertion error.")
	}
}

func VPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 1 {
		log.Printf(format, a...)
	}
	return
}
