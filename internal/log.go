// Copyright 2017 Vector Creations Ltd
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"context"
	"fmt"
	"github.com/pitabwire/frame"
	"io"
	"net/http"
	"os"
	"path"
	"runtime"
	"strings"
)

// callerPrettyfier is a function that given a runtime.Frame object, will
// extract the calling function's name and file, and return them in a nicely
// formatted way
func callerPrettyfier(f *runtime.Frame) (string, string) {
	// Retrieve just the function name
	s := strings.Split(f.Function, ".")
	funcname := s[len(s)-1]

	// Append a newline + tab to it to move the actual log content to its own line
	funcname += "\n\t"

	// Use a shortened file path which just has the filename to avoid having lots of redundant
	// directories which contribute significantly to overall log sizes!
	filename := fmt.Sprintf(" [%s:%d]", path.Base(f.File), f.Line)

	return funcname, filename
}

// SetupPprof starts a pprof listener. We use the DefaultServeMux here because it is
// simplest, and it gives us the freedom to run pprof on a separate port.
func SetupPprof(ctx context.Context) {
	if hostPort := os.Getenv("PPROFLISTEN"); hostPort != "" {
		frame.Log(ctx).Warn("Starting pprof on ", hostPort)
		go func() {
			frame.Log(ctx).WithError(http.ListenAndServe(hostPort, nil)).Error("Failed to setup pprof listener")
		}()
	}
}

// CloseAndLogIfError Closes io.Closer and logs the error if any
func CloseAndLogIfError(ctx context.Context, closer io.Closer, message string) {
	if closer == nil {
		return
	}
	err := closer.Close()
	if ctx == nil {
		ctx = context.TODO()
	}
	if err != nil {
		frame.Log(ctx).WithError(err).Error(message)
	}
}
