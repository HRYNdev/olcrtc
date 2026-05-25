//go:build mage

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

const (
	module    = "github.com/openlibrecommunity/olcrtc"
	buildDir  = "build"
	ldflags   = "-s -w"
	goVersion = "1.25"
)

var (
	goexe  = mg.GoCmd()
	goos   = envOr("GOOS", runtime.GOOS)
	goarch = envOr("GOARCH", runtime.GOARCH)
)

// Build builds the olcrtc CLI binary.
func Build() error {
	mg.Deps(BuildCLI)
	return nil
}

// BuildCLI builds the olcrtc server/client binary.
func BuildCLI() error {
	mg.Deps(Deps)
	return buildBinary("olcrtc", "./cmd/olcrtc", goos, goarch)
}

// Cross builds olcrtc for all supported platforms.
func Cross() error {
	mg.Deps(Deps)

	targets := []struct{ os, arch string }{
		{"linux", "amd64"},
		{"linux", "arm64"},
		{"windows", "amd64"},
		{"darwin", "amd64"},
		{"darwin", "arm64"},
		{"freebsd", "amd64"},
		{"freebsd", "arm64"},
		{"openbsd", "amd64"},
		{"openbsd", "arm64"},
	}

	for _, t := range targets {
		if err := buildBinary("olcrtc", "./cmd/olcrtc", t.os, t.arch); err != nil {
			return err
		}
	}

	fmt.Printf("✅ Built %d platform(s)\n", len(targets))
	return nil
}

// Podman builds the image using podman.
func Podman() error {
	tag := envOr("DOCKER_TAG", "olcrtc:latest")
	return sh.RunV("podman", "build", "-t", tag, ".")
}

// Docker builds the image using docker.
func Docker() error {
	tag := envOr("DOCKER_TAG", "olcrtc:latest")
	return sh.RunV("docker", "build", "-t", tag, ".")
}

// Lint runs golangci-lint.
func Lint() error {
	if err := ensureTool("golangci-lint"); err != nil {
		return fmt.Errorf("golangci-lint not found, install it:\n  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest")
	}
	return sh.RunV("golangci-lint", "run", "./...")
}

// Test runs all unit tests (short mode, skips long-running chaos/throughput tests).
func Test() error {
	return sh.RunV(goexe, "test", "-race", "-count=1", "-short", "./...")
}

// TestFull runs all tests including chaos and throughput baselines (no real providers).
func TestFull() error {
	return sh.RunV(goexe, "test", "-race", "-count=1", "-timeout", "10m", "./...")
}

// E2E runs the real-provider e2e matrix plus stress tests.
// Configure via env: E2E_CARRIERS, E2E_TRANSPORTS, E2E_TIMEOUT, E2E_STRESS.
func E2E() error {
	args := []string{"test", "-race", "-count=1", "-v", "-timeout", "30m"}
	args = append(args, "-olcrtc.real-e2e=true")
	if carriers := os.Getenv("E2E_CARRIERS"); carriers != "" {
		args = append(args, "-olcrtc.real-carriers="+carriers)
	}
	if transports := os.Getenv("E2E_TRANSPORTS"); transports != "" {
		args = append(args, "-olcrtc.real-transports="+transports)
	}
	if timeout := os.Getenv("E2E_TIMEOUT"); timeout != "" {
		args = append(args, "-olcrtc.real-timeout="+timeout)
	}
	if os.Getenv("E2E_STRESS") != "" {
		args = append(args, "-olcrtc.stress=true")
		if d := os.Getenv("E2E_STRESS_DURATION"); d != "" {
			args = append(args, "-olcrtc.stress-duration="+d)
		}
	}
	args = append(args, "./internal/e2e/...")
	return sh.RunV(goexe, args...)
}

// Soak runs the real-provider throughput soak test.
// Configure via env: SOAK_CARRIERS, SOAK_TRANSPORTS, SOAK_DURATION.
func Soak() error {
	carriers := envOr("SOAK_CARRIERS", "telemost,jitsi,wbstream")
	transports := envOr("SOAK_TRANSPORTS", "datachannel,vp8channel")
	duration := envOr("SOAK_DURATION", "10m")

	args := []string{"test", "-count=1", "-v",
		"-timeout", "4h",
		"-olcrtc.real-e2e=true",
		"-olcrtc.real-soak=true",
		"-olcrtc.real-soak-carrier=" + carriers,
		"-olcrtc.real-soak-transport=" + transports,
		"-olcrtc.real-soak-duration=" + duration,
		"-run", "^TestRealThroughputSoak$",
		"./internal/e2e/...",
	}
	return sh.RunV(goexe, args...)
}

// LocalSoak runs the local (in-memory) throughput soak.
// Configure via env: SOAK_TRANSPORTS, SOAK_DURATION, SOAK_CHAOS.
func LocalSoak() error {
	transports := envOr("SOAK_TRANSPORTS", "all")
	duration := envOr("SOAK_DURATION", "6m")
	chaos := os.Getenv("SOAK_CHAOS")

	args := []string{"test", "-count=1", "-v",
		"-timeout", "4h",
		"-olcrtc.local-soak=true",
		"-olcrtc.local-soak-transport=" + transports,
		"-olcrtc.local-soak-duration=" + duration,
		"-run", "^TestLocalThroughputSoak$",
	}
	if chaos != "" {
		args = append(args, "-olcrtc.local-soak-chaos="+chaos)
	}
	args = append(args, "./internal/e2e/...")
	return sh.RunV(goexe, args...)
}

// Deps downloads and tidies Go module dependencies.
func Deps() error {
	if err := sh.RunV(goexe, "mod", "download"); err != nil {
		return err
	}
	return sh.RunV(goexe, "mod", "tidy")
}

// Clean removes build artifacts.
func Clean() error {
	return os.RemoveAll(buildDir)
}

// Mobile builds the Android AAR via gomobile.
func Mobile() error {
	if err := ensureTool("gomobile"); err != nil {
		return fmt.Errorf("gomobile not found: run 'go install golang.org/x/mobile/cmd/gomobile@latest && gomobile init'")
	}
	if err := ensureBuildDir(); err != nil {
		return err
	}
	return sh.RunV("gomobile", "bind",
		"-target=android",
		"-androidapi", "21",
		"-ldflags", "-s -w -checklinkname=0",
		"-o", filepath.Join(buildDir, "olcrtc.aar"),
		"./mobile",
	)
}

func buildBinary(name, pkg, os_, arch string) error {
	if err := ensureBuildDir(); err != nil {
		return err
	}

	ext := ""
	if os_ == "windows" {
		ext = ".exe"
	}
	outName := fmt.Sprintf("%s-%s-%s%s", name, os_, arch, ext)
	out := filepath.Join(buildDir, outName)
	fmt.Printf("building %s (%s/%s) -> %s\n", name, os_, arch, out)

	env := map[string]string{
		"GOOS":        os_,
		"GOARCH":      arch,
		"CGO_ENABLED": "0",
	}

	flags := ldflags
	if os_ == "android" {
		flags += " -checklinkname=0"
	}

	args := []string{"build", "-trimpath", "-ldflags", flags, "-o", out, pkg}

	return sh.RunWithV(env, goexe, args...)
}

func ensureBuildDir() error {
	return os.MkdirAll(buildDir, 0o755)
}

func ensureTool(name string) error {
	_, err := exec.LookPath(name)
	return err
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
