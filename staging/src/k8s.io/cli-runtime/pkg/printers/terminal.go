/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package printers

import (
	"io"
	"os"
	"runtime"
	"strings"

	"github.com/moby/term"
)

// terminalEscaper replaces ANSI escape sequences and other terminal special
// characters to avoid terminal escape character attacks (issue #101695).
var terminalEscaper = strings.NewReplacer("\x1b", "^[", "\r", "\\r")

// WriteEscaped replaces unsafe terminal characters with replacement strings
// and writes them to the given writer.
func WriteEscaped(writer io.Writer, output string) error {
	_, err := terminalEscaper.WriteString(writer, output)
	return err
}

// EscapeTerminal escapes terminal special characters in a human readable (but
// non-reversible) format.
func EscapeTerminal(in string) string {
	return terminalEscaper.Replace(in)
}

// IsTerminal returns whether the passed object is a terminal or not
// 检查传入的对象是否是一个终端（Terminal）。在具体实现中，它调用了 term.GetFdInfo(i) 来获取文件描述符信息，并通过判断该对象是否与终端相关来返回结果。
func IsTerminal(i interface{}) bool {
	_, terminal := term.GetFdInfo(i)
	return terminal
}

// AllowsColorOutput returns true if the specified writer is a terminal and
// the process environment indicates color output is supported and desired.
func AllowsColorOutput(w io.Writer) bool {
	//检查传入的对象是否是一个终端（Terminal）。在具体实现中，它调用了 term.GetFdInfo(i) 来获取文件描述符信息，并通过判断该对象是否与终端相关来返回结果。
	if !IsTerminal(w) {
		return false
	}

	// https://en.wikipedia.org/wiki/Computer_terminal#Dumb_terminals
	//TERM 环境变量指示当前终端的类型。如果终端类型是 "dumb"，
	//意味着终端不支持颜色等高级特性（常见于非常基础或老旧的终端）。
	//因此，若 TERM 是 "dumb"，则不允许彩色输出，返回 false。
	if os.Getenv("TERM") == "dumb" {
		return false
	}

	// https://no-color.org/
	if _, nocolor := os.LookupEnv("NO_COLOR"); nocolor {
		return false
	}

	// On Windows WT_SESSION is set by the modern terminal component.
	// Older terminals have poor support for UTF-8, VT escape codes, etc.
	//Windows 系统中，新的终端（如 Windows Terminal）会设置 WT_SESSION 环境变量。
	//老旧的终端（如 cmd 或 PowerShell）对 UTF-8 和 ANSI 转义序列（用于输出彩色文本）的支持较差。
	//如果当前是 Windows 系统，并且环境变量 WT_SESSION 为空，意味着用户没有使用现代终端，因此返回 false，
	//不允许彩色输出。
	if runtime.GOOS == "windows" && os.Getenv("WT_SESSION") == "" {
		return false
	}

	return true
}
