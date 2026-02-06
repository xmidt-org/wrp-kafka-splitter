<!--SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
SPDX-License-Identifier: Apache-2.0 -->

<!-- OPENSPEC:START -->
# OpenSpec Instructions

These instructions are for AI assistants working in this project.

Always open `@/openspec/AGENTS.md` when the request:
- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/openspec/AGENTS.md` to learn:
- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

Keep this managed block so 'openspec update' can refresh the instructions.

<!-- OPENSPEC:END -->

# AI Agent Guidelines for Go Development

This document provides guidelines for AI agents working with this Go repository. Following these practices ensures consistent, high-quality code generation and maintenance.

## Project Structure

### Standard Go Layout
- Follow the [Standard Go Project Layout](https://github.com/golang-standards/project-layout)
- Place application code in `/cmd` for executables
- Store reusable library code in `/pkg`
- Keep internal packages in `/internal` (not importable by external projects)
- Use `/api` for API definitions (OpenAPI/Swagger, Protocol Buffers, etc.)
- Place build scripts and configurations in `/build`
- Store deployment configurations in `/deployments`

### Module Management
- Always use Go modules (`go.mod` and `go.sum`)
- Run `go mod tidy` after adding or removing dependencies
- Use semantic versioning for module versions
- Prefer explicit dependency versions over `latest`

## Code Style and Formatting

### Formatting
- All code must be formatted with `gofmt` or `goimports`
- Use `goimports` to automatically manage import statements
- Run `go vet` to catch common errors
- Use `golangci-lint` for comprehensive linting

### Naming Conventions
- Use `camelCase` for unexported identifiers
- Use `PascalCase` for exported identifiers
- Keep names concise but meaningful (prefer `userCount` over `numberOfUsers`)
- Use single-letter names only for short-lived variables (loop counters, receivers)
- Name interfaces with `-er` suffix for single-method interfaces (e.g., `Reader`, `Writer`)
- Avoid stuttering in names (prefer `user.Name` over `user.UserName` in package `user`)

### Package Organization
- Keep package names lowercase, single-word when possible
- Package names should be descriptive of their purpose
- Avoid generic names like `util`, `common`, or `helpers`
- One package per directory

## Error Handling

### Error Patterns
- Always check and handle errors explicitly
- Use `errors.New()` or `fmt.Errorf()` for simple errors
- Wrap errors with context using `fmt.Errorf("context: %w", err)` (Go 1.13+)
- Create custom error types for errors that need special handling
- Return errors as the last return value
- Don't panic except in truly exceptional circumstances (initialization failures)

### Error Messages
- Start error messages with lowercase (they're often wrapped)
- Don't end error messages with punctuation
- Provide sufficient context in error messages
- Include relevant variable values in error messages

## Testing

### Test Structure
- Place tests in the same package as the code (use `_test.go` suffix)
- Use one test file per source `_.go` file.  Do not split tests into multiple files for a single source file.
- Use test suites (use library "testify/suite") to set up tests and break them down. 
- Use `package <name>_test` for black-box testing
- Name test functions `TestXxx` where Xxx describes what's being tested
- Use table-driven tests for testing multiple scenarios
- Use subtests (`t.Run()`) to organize related test cases
- for mocks, use the mock package from "github.com/stretchr/testify".  Mocks should go 
  in the same directory as the tests in a file called "mocks_test.go". 

### Test Quality
- Aim for high test coverage (80%+ for critical paths)
- Test both happy paths and error cases
- Use `testing.T` for unit tests, `testing.B` for benchmarks
- Mock external dependencies using interfaces
- Write meaningful test failure messages
- Use `testify` or similar libraries for assertions when appropriate
- Use test suites

### Benchmark and Examples
- Benchmark performance-critical code
- Include example functions for documentation (`Example` prefix)
- Examples should have `// Output:` comments for verification

## Concurrency

### Goroutines and Channels
- Use goroutines for concurrent operations, not parallel loops by default
- Always ensure goroutines can exit (avoid leaks)
- Use `context.Context` for cancellation and timeouts
- Prefer channels for communication, mutexes for state protection
- Close channels only from the sender side
- Use buffered channels judiciously (understand backpressure)

### Synchronization
- Use `sync.WaitGroup` to wait for multiple goroutines
- Use `sync.Once` for one-time initialization
- Prefer `sync.RWMutex` when reads significantly outnumber writes
- Document which goroutine owns which data
- Use the race detector (`go test -race`) during development

## Documentation

### Code Comments
- Write godoc-style comments for all exported identifiers
- Start comments with the name of the element being described
- Use complete sentences in documentation comments
- Explain "why" in comments, not "what" (code should be self-explanatory)
- Keep comments up-to-date with code changes

### Package Documentation
- Include a package comment in one file (typically `doc.go` for complex packages)
- Provide usage examples in package documentation
- Document any non-obvious behavior or limitations

## Performance and Optimization

### General Practices
- Profile before optimizing (`pprof`, benchmarks)
- Avoid premature optimization
- Use appropriate data structures (maps, slices, arrays)
- Pre-allocate slices when size is known: `make([]Type, 0, capacity)`
- Reuse buffers with `sync.Pool` for frequently allocated objects
- Be mindful of memory allocations in hot paths

### Resource Management
- Always close resources (`defer file.Close()`)
- Use `defer` for cleanup operations
- Place `defer` immediately after successful resource acquisition
- Be aware of `defer` in loops (may want to use a closure)
- Use `context.Context` for request-scoped values and cancellation

## Dependencies and Security

### Dependency Management
- Minimize external dependencies
- Vet dependencies for maintenance and security
- Pin dependency versions in `go.mod`
- Regularly update dependencies (`go get -u`)
- Use `go mod verify` to check dependency integrity

### Security Practices
- Validate and sanitize all inputs
- Use prepared statements for database queries
- Don't log sensitive information
- Use `crypto/rand` for cryptographic operations, not `math/rand`
- Keep secrets in environment variables or secure vaults, never in code
- Run security scanners (`gosec`, `nancy`) in CI/CD

## API Design

### Interface Design
- Keep interfaces small (single method when possible)
- Accept interfaces, return concrete types
- Define interfaces where they're used, not where they're implemented
- Use `io.Reader` and `io.Writer` when applicable

### Function Design
- Keep functions small and focused on a single responsibility
- Limit function parameters (consider a config struct for many parameters)
- Use functional options pattern for optional parameters
- Return early to reduce nesting

## Configuration

### Environment and Config
- Use environment variables for deployment-specific configuration
- Support configuration files (YAML, JSON, TOML) for complex setups
- Provide sensible defaults
- Validate configuration at startup
- Use libraries like `https://github.com/goschtalt` for complex configuration
- Use libraries like `alecthomas/kong` for parsing command line arguments

## Logging

### Logging Practices
- Use an observer pattern to log events so the code is not tied to a specific logger. Components should notify a log listeners to log events. 
- Use structured logging (`slog`)
- Log at appropriate levels (DEBUG, INFO, WARN, ERROR)
- Include context in log messages (request IDs, user IDs)
- Don't log sensitive information
- Make logs machine-readable and grep-friendly

## Build and Deployment

### Build Process
- Use `make` or task runners for build automation
- Support cross-compilation when needed
- Use build tags for platform-specific code
- Include version information in binaries (`-ldflags`)
- Create reproducible builds

### Docker and Containers
- Use multi-stage builds to minimize image size
- Run as non-root user in containers
- Use scratch or distroless base images for production
- Copy only necessary files into the image
- Use `.dockerignore` to exclude unnecessary files

## CI/CD Integration

### Continuous Integration
- Run tests on every commit
- Include linting and formatting checks
- Run the race detector in CI
- Check test coverage
- Validate `go.mod` and `go.sum` consistency
- Build for all target platforms

### Code Quality Gates
- Enforce minimum test coverage thresholds
- Require all linters to pass
- Run security scanners
- Validate documentation completeness

## Git and Version Control

### Commit Practices
- Write clear, descriptive commit messages
- Keep commits atomic and focused
- Reference issue numbers in commit messages
- Use conventional commits format when applicable

### Branch Strategy
- Use feature branches for development
- Keep the main branch stable and deployable
- Squash commits when merging if appropriate
- Delete merged branches

## Common Patterns

### Functional Options
```go
type Config struct {
    timeout time.Duration
    retries int
}

type Option func(*Config)

func WithTimeout(d time.Duration) Option {
    return func(c *Config) { c.timeout = d }
}

func NewService(opts ...Option) *Service {
    cfg := &Config{timeout: 30 * time.Second, retries: 3}
    for _, opt := range opts {
        opt(cfg)
    }
    return &Service{config: cfg}
}
```

### Builder Pattern
Use for complex object construction with validation

### Context Propagation
Always pass `context.Context` as the first parameter to functions that may block or need cancellation

## Tools and Utilities

### Essential Tools
- `go fmt` / `goimports` - formatting
- `go vet` - static analysis
- `golangci-lint` - comprehensive linting
- `go test -race` - race detection
- `go test -cover` - coverage analysis
- `pprof` - profiling
- `gosec` - security scanning
- `staticcheck` - additional static analysis

### IDE/Editor Setup
- Configure editor to run `goimports` on save
- Enable inline error checking
- Set up debugging support
- Install Go extension/plugin for your editor

## Language-Specific Best Practices

### Zero Values
- Design types so their zero value is useful
- Document when zero value is not ready to use

### Embedding
- Use struct embedding for composition
- Understand promoted methods and fields
- Avoid embedding for "has-a" relationships

### Type Conversions
- Be explicit with type conversions
- Use type aliases for clarity, not just brevity

### Pointers
- Use pointers for large structs to avoid copies
- Use pointers when you need to modify the receiver
- Don't use pointers for performance unless profiling shows it matters

## Anti-Patterns to Avoid

- Don't use `init()` functions except when absolutely necessary
- Avoid global variables and package-level state
- Don't ignore errors (use `_` sparingly and only when justified)
- Don't use panic/recover for normal error handling
- Avoid reflection unless absolutely necessary
- Don't export unnecessarily (keep APIs minimal)
- Avoid premature abstraction
- Don't use channels when a mutex would suffice
- Avoid context.Value for anything except request-scoped data

## Resources

- [Effective Go](https://golang.org/doc/effective_go)
- [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md)
- [Go Proverbs](https://go-proverbs.github.io/)
- [Standard Go Project Layout](https://github.com/golang-standards/project-layout)

---

*This document should be regularly updated as the project evolves and new best practices emerge.*
