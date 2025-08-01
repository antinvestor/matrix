// Copyright 2022 The Global.org Foundation C.I.C.
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

package util

import (
	"context"
	"syscall"
	"time"

	"github.com/pitabwire/util"
)

func getMemoryStats(ctx context.Context, p *phoneHomeStats) error {
	oldUsage := p.prevData
	newUsage := syscall.Rusage{}
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &newUsage); err != nil {
		util.Log(ctx).WithError(err).Error("unable to get usage")
		return err
	}
	newData := timestampToRUUsage{timestamp: time.Now().Unix(), usage: newUsage}
	p.prevData = newData

	usedCPUTime := (newUsage.Utime.Sec + newUsage.Stime.Sec) - (oldUsage.usage.Utime.Sec + oldUsage.usage.Stime.Sec)

	if usedCPUTime == 0 || newData.timestamp == oldUsage.timestamp {
		p.stats["cpu_average"] = 0
	} else {
		// conversion to int64 required for GOARCH=386
		p.stats["cpu_average"] = usedCPUTime / (newData.timestamp - oldUsage.timestamp) * 100
	}
	p.stats["memory_rss"] = newUsage.Maxrss
	return nil
}
