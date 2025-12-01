# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.x     | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in Clotho-Engine, please report it responsibly:

1. **Do NOT** open a public GitHub issue
2. Email the maintainer directly at: [e35zhang@uwaterloo.ca]
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)

We will acknowledge receipt within 48 hours and provide a detailed response within 7 days.

## Security Considerations

Clotho-Engine includes an **Expression Sandbox** that evaluates user-defined expressions safely:

- Only whitelisted functions are allowed (`sum`, `len`, `min`, `max`, etc.)
- No `eval()`, `exec()`, or `__import__` access
- Attribute access is restricted to prevent sandbox escape

See `tests/part2_types/test_expression_security.py` for security test coverage.
