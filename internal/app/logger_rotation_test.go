// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLogger_WithRotation(t *testing.T) {
	// Create a temporary directory for test logs
	tmpDir, err := os.MkdirTemp("", "splitter-log-rotation-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logFile := filepath.Join(tmpDir, "test.log")

	cfg := LogConfig{
		Level:       "INFO",
		Encoding:    "json",
		OutputPaths: []string{logFile},
		Rotation: RotationConfig{
			MaxSize:    1, // 1 MB for quick testing
			MaxAge:     1, // 1 day
			MaxBackups: 2, // Keep 2 backups
			Compress:   true,
			LocalTime:  true,
		},
	}

	logger, err := newLogger(cfg)
	require.NoError(t, err)
	require.NotNil(t, logger)

	// Log some messages
	logger.Info("test message 1", "key", "value1")
	logger.Info("test message 2", "key", "value2")
	logger.Error("test error", "error", "something went wrong")

	// Give it a moment to write
	time.Sleep(100 * time.Millisecond)

	// Verify log file was created
	assert.FileExists(t, logFile)

	// Read and verify content
	content, err := os.ReadFile(logFile)
	require.NoError(t, err)
	assert.Contains(t, string(content), "test message 1")
	assert.Contains(t, string(content), "test message 2")
	assert.Contains(t, string(content), "test error")
}

func TestNewLogger_MultipleOutputsWithRotation(t *testing.T) {
	// Create a temporary directory for test logs
	tmpDir, err := os.MkdirTemp("", "splitter-log-multi-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logFile1 := filepath.Join(tmpDir, "test1.log")
	logFile2 := filepath.Join(tmpDir, "test2.log")

	cfg := LogConfig{
		Level:       "DEBUG",
		Encoding:    "console",
		OutputPaths: []string{"stdout", logFile1, logFile2},
		Rotation: RotationConfig{
			MaxSize:    5,
			MaxAge:     7,
			MaxBackups: 3,
			Compress:   false,
			LocalTime:  false,
		},
	}

	logger, err := newLogger(cfg)
	require.NoError(t, err)
	require.NotNil(t, logger)

	// Log a test message
	logger.Debug("debug message for multiple outputs")

	// Give it a moment to write
	time.Sleep(100 * time.Millisecond)

	// Both log files should exist and contain the message
	assert.FileExists(t, logFile1)
	assert.FileExists(t, logFile2)

	content1, err := os.ReadFile(logFile1)
	require.NoError(t, err)
	assert.Contains(t, string(content1), "debug message for multiple outputs")

	content2, err := os.ReadFile(logFile2)
	require.NoError(t, err)
	assert.Contains(t, string(content2), "debug message for multiple outputs")
}

func TestNewLogger_RotationConfigDefaults(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "splitter-log-defaults-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logFile := filepath.Join(tmpDir, "defaults.log")

	cfg := LogConfig{
		Level:       "WARN",
		OutputPaths: []string{logFile},
		// Rotation config not specified - should use defaults
	}

	logger, err := newLogger(cfg)
	require.NoError(t, err)
	require.NotNil(t, logger)

	logger.Warn("warning with default rotation config")

	// Give it a moment to write
	time.Sleep(100 * time.Millisecond)

	assert.FileExists(t, logFile)
	content, err := os.ReadFile(logFile)
	require.NoError(t, err)
	assert.Contains(t, string(content), "warning with default rotation config")
}

func TestRotationConfig_DefaultValues(t *testing.T) {
	// Test that rotation config struct has sensible zero values
	var cfg RotationConfig

	// These should be the zero values, which are handled by lumberjack defaults
	assert.Equal(t, 0, cfg.MaxSize)
	assert.Equal(t, 0, cfg.MaxAge)
	assert.Equal(t, 0, cfg.MaxBackups)
	assert.False(t, cfg.Compress)
	assert.False(t, cfg.LocalTime)
}
