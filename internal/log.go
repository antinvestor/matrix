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
	"io"
	"net/http"
	"os"

	"github.com/pitabwire/util"
)

// SetupPprof starts a pprof listener. We use the DefaultServeMux here because it is
// simplest, and it gives us the freedom to run pprof on a separate port.
func SetupPprof(ctx context.Context) {
	if hostPort := os.Getenv("PPROFLISTEN"); hostPort != "" {
		util.Log(ctx).Warn("Starting pprof on ", hostPort)
		go func() {
			util.Log(ctx).WithError(http.ListenAndServe(hostPort, nil)).Error("Failed to setup pprof listener")
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
		util.Log(ctx).WithError(err).Error(message)
	}
}
