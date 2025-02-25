package internal

import (
	"runtime/debug"
	"strings"
)

// the final version string
var version string

// -ldflags "-X github.com/antinvestor/matrix/internal.branch=main"
var branch string

// -ldflags "-X github.com/antinvestor/matrix/internal.build=alpha"
var build string

// -ldflags "-X github.com/antinvestor/matrix/internal.versionTag=v0.6.3"
var versionTag = "v0.0.0"

const (
	gitRevLen = 7 // 7 matches the displayed characters on github.com
)

func VersionString() string {
	return version
}

func init() {

	version = versionTag
	var parts []string
	if build != "" {
		parts = append(parts, build)
	}
	if branch != "" {
		parts = append(parts, branch)
	}

	defer func() {
		if len(parts) > 0 {
			version += "+" + strings.Join(parts, ".")
		}
	}()

	// Try to get the revision Dendrite was build from.
	// If we can't, e.g. Dendrite wasn't built (go run) or no VCS version is present,
	// we just use the provided version above.
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}

	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			revLen := len(setting.Value)
			if revLen >= gitRevLen {
				parts = append(parts, setting.Value[:gitRevLen])
			} else {
				parts = append(parts, setting.Value[:revLen])
			}
			break
		}
	}
}
