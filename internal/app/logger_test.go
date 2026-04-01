// SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx/fxevent"
)

type LoggerTestSuite struct {
	suite.Suite
	tempDir string
}

func (s *LoggerTestSuite) SetupTest() {
	var err error
	s.tempDir, err = os.MkdirTemp("", "logger_test_*")
	s.NoError(err)
}

func (s *LoggerTestSuite) TearDownTest() {
	if s.tempDir != "" {
		os.RemoveAll(s.tempDir)
	}
}

// Test newLogger with default configuration
func (s *LoggerTestSuite) TestNewLogger_DefaultConfig() {
	cfg := LogConfig{
		Level:       "INFO",
		Encoding:    "json",
		OutputPaths: []string{},
	}

	logger, err := newLogger(cfg)
	s.NoError(err)
	s.NotNil(logger)
}

// Test newLogger with DEBUG level
func (s *LoggerTestSuite) TestNewLogger_DebugLevel() {
	cfg := LogConfig{
		Level:       "DEBUG",
		Encoding:    "json",
		OutputPaths: []string{"stdout"},
	}

	logger, err := newLogger(cfg)
	s.NoError(err)
	s.NotNil(logger)
}

// Test newLogger with WARN level
func (s *LoggerTestSuite) TestNewLogger_WarnLevel() {
	cfg := LogConfig{
		Level:       "WARN",
		Encoding:    "json",
		OutputPaths: []string{"stdout"},
	}

	logger, err := newLogger(cfg)
	s.NoError(err)
	s.NotNil(logger)
}

// Test newLogger with ERROR level
func (s *LoggerTestSuite) TestNewLogger_ErrorLevel() {
	cfg := LogConfig{
		Level:       "ERROR",
		Encoding:    "json",
		OutputPaths: []string{"stdout"},
	}

	logger, err := newLogger(cfg)
	s.NoError(err)
	s.NotNil(logger)
}

// Test newLogger with unknown level defaults to INFO
func (s *LoggerTestSuite) TestNewLogger_UnknownLevel() {
	cfg := LogConfig{
		Level:       "UNKNOWN",
		Encoding:    "json",
		OutputPaths: []string{"stdout"},
	}

	logger, err := newLogger(cfg)
	s.NoError(err)
	s.NotNil(logger)
}

// Test newLogger with console encoding
func (s *LoggerTestSuite) TestNewLogger_ConsoleEncoding() {
	cfg := LogConfig{
		Level:       "INFO",
		Encoding:    "console",
		OutputPaths: []string{"stdout"},
	}

	logger, err := newLogger(cfg)
	s.NoError(err)
	s.NotNil(logger)
}

// Test newLogger with JSON encoding
func (s *LoggerTestSuite) TestNewLogger_JSONEncoding() {
	cfg := LogConfig{
		Level:       "INFO",
		Encoding:    "json",
		OutputPaths: []string{"stdout"},
	}

	logger, err := newLogger(cfg)
	s.NoError(err)
	s.NotNil(logger)
}

// Test newLogger with stdout output
func (s *LoggerTestSuite) TestNewLogger_StdoutOutput() {
	cfg := LogConfig{
		Level:       "INFO",
		Encoding:    "json",
		OutputPaths: []string{"stdout"},
	}

	logger, err := newLogger(cfg)
	s.NoError(err)
	s.NotNil(logger)
}

// Test newLogger with stderr output
func (s *LoggerTestSuite) TestNewLogger_StderrOutput() {
	cfg := LogConfig{
		Level:       "INFO",
		Encoding:    "json",
		OutputPaths: []string{"stderr"},
	}

	logger, err := newLogger(cfg)
	s.NoError(err)
	s.NotNil(logger)
}

// Test newLogger with empty string output (should default to stdout)
func (s *LoggerTestSuite) TestNewLogger_EmptyStringOutput() {
	cfg := LogConfig{
		Level:       "INFO",
		Encoding:    "json",
		OutputPaths: []string{""},
	}

	logger, err := newLogger(cfg)
	s.NoError(err)
	s.NotNil(logger)
}

// Test newLogger with file output
func (s *LoggerTestSuite) TestNewLogger_FileOutput() {
	logFile := filepath.Join(s.tempDir, "test.log")
	cfg := LogConfig{
		Level:       "INFO",
		Encoding:    "json",
		OutputPaths: []string{logFile},
	}

	logger, err := newLogger(cfg)
	s.NoError(err)
	s.NotNil(logger)

	// Log a message to trigger file creation
	logger.Info("test message")

	// Verify file was created
	s.FileExists(logFile)
}

// Test newLogger with invalid file path
func (s *LoggerTestSuite) TestNewLogger_InvalidFilePath() {
	invalidPath := filepath.Join(s.tempDir, "nonexistent", "dir", "test.log")
	cfg := LogConfig{
		Level:       "INFO",
		Encoding:    "json",
		OutputPaths: []string{invalidPath},
	}

	// Logger creation succeeds with lumberjack, but writing will fail
	logger, err := newLogger(cfg)
	s.NoError(err)
	s.NotNil(logger)

	// The error will occur when we try to write, not during logger creation
	// This is expected behavior with lumberjack
}

// Test newLogger with multiple outputs
func (s *LoggerTestSuite) TestNewLogger_MultipleOutputs() {
	logFile1 := filepath.Join(s.tempDir, "test1.log")
	logFile2 := filepath.Join(s.tempDir, "test2.log")

	cfg := LogConfig{
		Level:       "INFO",
		Encoding:    "json",
		OutputPaths: []string{"stdout", logFile1, "stderr", logFile2},
	}

	logger, err := newLogger(cfg)
	s.NoError(err)
	s.NotNil(logger)

	// Log a message to trigger file creation
	logger.Info("test message")

	// Verify files were created
	s.FileExists(logFile1)
	s.FileExists(logFile2)
}

// Test newLogger with multiple outputs including one invalid path
func (s *LoggerTestSuite) TestNewLogger_MultipleOutputs_WithInvalidPath() {
	logFile := filepath.Join(s.tempDir, "test.log")
	invalidPath := filepath.Join(s.tempDir, "nonexistent", "dir", "test.log")

	cfg := LogConfig{
		Level:       "INFO",
		Encoding:    "json",
		OutputPaths: []string{"stdout", logFile, invalidPath},
	}

	// Logger creation will succeed with lumberjack even with invalid paths
	logger, err := newLogger(cfg)
	s.NoError(err)
	s.NotNil(logger)

	// Log a message - some outputs may fail silently but valid ones should work
	logger.Info("test message")

	// At least the valid log file should be created
	s.FileExists(logFile)
}

// Test newLogger logs message to file
func (s *LoggerTestSuite) TestNewLogger_LogsToFile() {
	logFile := filepath.Join(s.tempDir, "test.log")
	cfg := LogConfig{
		Level:       "INFO",
		Encoding:    "json",
		OutputPaths: []string{logFile},
	}

	logger, err := newLogger(cfg)
	s.NoError(err)

	logger.Info("test message", slog.String("key", "value"))

	// Read and verify log file content
	content, err := os.ReadFile(logFile)
	s.NoError(err)
	s.NotEmpty(content)

	// Parse JSON to verify structure
	var logEntry map[string]interface{}
	err = json.Unmarshal(content, &logEntry)
	s.NoError(err)
	s.Equal("test message", logEntry["msg"])
	s.Equal("value", logEntry["key"])
}

// Test newFxLogger creates logger
func (s *LoggerTestSuite) TestNewFxLogger() {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(handler)

	fxLogger := newFxLogger(logger)
	s.NotNil(fxLogger)
}

// Test slogFxLogger logs OnStartExecuting event
func (s *LoggerTestSuite) TestSlogFxLogger_OnStartExecuting() {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(handler)
	fxLogger := &slogFxLogger{logger: logger}

	event := &fxevent.OnStartExecuting{
		CallerName: "testCaller",
	}

	fxLogger.LogEvent(event)

	// Verify log was written
	s.NotEmpty(buf.String())
	var logEntry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &logEntry)
	s.NoError(err)
	s.Contains(logEntry["msg"], "OnStart hook executing")
}

// Test slogFxLogger logs OnStartExecuted event with error
func (s *LoggerTestSuite) TestSlogFxLogger_OnStartExecuted_WithError() {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelError})
	logger := slog.New(handler)
	fxLogger := &slogFxLogger{logger: logger}

	event := &fxevent.OnStartExecuted{
		CallerName: "testCaller",
		Err:        fmt.Errorf("test error"),
	}

	fxLogger.LogEvent(event)

	// Verify error was logged
	s.NotEmpty(buf.String())
	var logEntry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &logEntry)
	s.NoError(err)
	s.Contains(logEntry["msg"], "OnStart hook failed")
}

// Test slogFxLogger logs OnStartExecuted event without error
func (s *LoggerTestSuite) TestSlogFxLogger_OnStartExecuted_WithoutError() {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(handler)
	fxLogger := &slogFxLogger{logger: logger}

	event := &fxevent.OnStartExecuted{
		CallerName: "testCaller",
		Err:        nil,
	}

	fxLogger.LogEvent(event)

	// Verify success was logged
	s.NotEmpty(buf.String())
	var logEntry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &logEntry)
	s.NoError(err)
	s.Contains(logEntry["msg"], "OnStart hook executed")
}

// Test slogFxLogger logs OnStopExecuting event
func (s *LoggerTestSuite) TestSlogFxLogger_OnStopExecuting() {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(handler)
	fxLogger := &slogFxLogger{logger: logger}

	event := &fxevent.OnStopExecuting{
		CallerName: "testCaller",
	}

	fxLogger.LogEvent(event)

	// Verify log was written
	s.NotEmpty(buf.String())
	var logEntry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &logEntry)
	s.NoError(err)
	s.Contains(logEntry["msg"], "OnStop hook executing")
}

// Test slogFxLogger logs OnStopExecuted event with error
func (s *LoggerTestSuite) TestSlogFxLogger_OnStopExecuted_WithError() {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelError})
	logger := slog.New(handler)
	fxLogger := &slogFxLogger{logger: logger}

	event := &fxevent.OnStopExecuted{
		CallerName: "testCaller",
		Err:        fmt.Errorf("test error"),
	}

	fxLogger.LogEvent(event)

	// Verify error was logged
	s.NotEmpty(buf.String())
	var logEntry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &logEntry)
	s.NoError(err)
	s.Contains(logEntry["msg"], "OnStop hook failed")
}

// Test slogFxLogger logs OnStopExecuted event without error
func (s *LoggerTestSuite) TestSlogFxLogger_OnStopExecuted_WithoutError() {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(handler)
	fxLogger := &slogFxLogger{logger: logger}

	event := &fxevent.OnStopExecuted{
		CallerName: "testCaller",
		Err:        nil,
	}

	fxLogger.LogEvent(event)

	// Verify success was logged
	s.NotEmpty(buf.String())
	var logEntry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &logEntry)
	s.NoError(err)
	s.Contains(logEntry["msg"], "OnStop hook executed")
}

// Test slogFxLogger logs Supplied event
func (s *LoggerTestSuite) TestSlogFxLogger_Supplied() {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(handler)
	fxLogger := &slogFxLogger{logger: logger}

	event := &fxevent.Supplied{
		TypeName: "testType",
	}

	fxLogger.LogEvent(event)

	// Verify log was written
	s.NotEmpty(buf.String())
	var logEntry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &logEntry)
	s.NoError(err)
	s.Contains(logEntry["msg"], "supplied")
}

// Test slogFxLogger logs Provided event
func (s *LoggerTestSuite) TestSlogFxLogger_Provided() {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(handler)
	fxLogger := &slogFxLogger{logger: logger}

	event := &fxevent.Provided{
		ModuleName: "testModule",
	}

	fxLogger.LogEvent(event)

	// Verify log was written
	s.NotEmpty(buf.String())
	var logEntry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &logEntry)
	s.NoError(err)
	s.Contains(logEntry["msg"], "provided")
}

// Test slogFxLogger logs Invoked event
func (s *LoggerTestSuite) TestSlogFxLogger_Invoked() {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(handler)
	fxLogger := &slogFxLogger{logger: logger}

	event := &fxevent.Invoked{
		FunctionName: "testFunction",
	}

	fxLogger.LogEvent(event)

	// Verify log was written
	s.NotEmpty(buf.String())
	var logEntry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &logEntry)
	s.NoError(err)
	s.Contains(logEntry["msg"], "invoked")
}

// Test slogFxLogger logs Stopped event with error
func (s *LoggerTestSuite) TestSlogFxLogger_Stopped_WithError() {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelError})
	logger := slog.New(handler)
	fxLogger := &slogFxLogger{logger: logger}

	event := &fxevent.Stopped{
		Err: fmt.Errorf("test error"),
	}

	fxLogger.LogEvent(event)

	// Verify error was logged
	s.NotEmpty(buf.String())
	var logEntry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &logEntry)
	s.NoError(err)
	s.Contains(logEntry["msg"], "stopped")
}

// Test slogFxLogger logs Stopped event without error
func (s *LoggerTestSuite) TestSlogFxLogger_Stopped_WithoutError() {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(handler)
	fxLogger := &slogFxLogger{logger: logger}

	event := &fxevent.Stopped{
		Err: nil,
	}

	fxLogger.LogEvent(event)

	// Verify log was written
	s.NotEmpty(buf.String())
	var logEntry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &logEntry)
	s.NoError(err)
	s.Contains(logEntry["msg"], "stopped")
}

// Test slogFxLogger logs Started event
func (s *LoggerTestSuite) TestSlogFxLogger_Started() {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(handler)
	fxLogger := &slogFxLogger{logger: logger}

	event := &fxevent.Started{}

	fxLogger.LogEvent(event)

	// Verify log was written
	s.NotEmpty(buf.String())
	var logEntry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &logEntry)
	s.NoError(err)
	s.Contains(logEntry["msg"], "started")
}

// Test slogFxLogger logs unknown event
func (s *LoggerTestSuite) TestSlogFxLogger_UnknownEvent() {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(handler)
	fxLogger := &slogFxLogger{logger: logger}

	// Use a simple type assertion to create an unknown event type
	event := &fxevent.RolledBack{}

	fxLogger.LogEvent(event)

	// Verify log was written
	s.NotEmpty(buf.String())
	var logEntry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &logEntry)
	s.NoError(err)
	s.Contains(logEntry["msg"], "fx event")
}

func TestLoggerTestSuite(t *testing.T) {
	suite.Run(t, new(LoggerTestSuite))
}
