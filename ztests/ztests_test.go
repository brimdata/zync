//go:build ztests
// +build ztests

package ztests

import (
	"testing"

	"github.com/brimdata/zed/ztest"
)

func TestZtests(t *testing.T) { ztest.Run(t, ".") }
