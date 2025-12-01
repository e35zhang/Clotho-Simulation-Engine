# Contributing to Clotho-Engine

Thank you for your interest in contributing to Clotho! This document provides guidelines for contributing.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/Clotho-Simulation-Engine.git`
3. Create a virtual environment: `python -m venv .venv`
4. Activate it: `.venv\Scripts\activate` (Windows) or `source .venv/bin/activate` (Unix)
5. Install dependencies: `pip install -r requirements.txt`

## Running Tests

```bash
# Run all tests
pytest tests/ -v --ignore=tests/part3_correctness/test_verification_diff_viewer.py

# Run specific test module
pytest tests/part1_core/ -v

# Run with coverage
pip install pytest-cov
pytest tests/ --cov=core --ignore=tests/part3_correctness/test_verification_diff_viewer.py
```

## Code Style

- Follow PEP 8 guidelines
- Use type hints where appropriate
- Write docstrings for public functions and classes
- Keep functions focused and under 50 lines when possible

## Pull Request Process

1. Create a feature branch: `git checkout -b feature/your-feature-name`
2. Make your changes
3. Ensure all tests pass: `pytest tests/ -v`
4. Update documentation if needed
5. Submit a pull request with a clear description

## Reporting Issues

When reporting issues, please include:
- Python version (`python --version`)
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Relevant error messages or logs

## Areas for Contribution

- **More LTL operators**: Extend the temporal logic engine
- **Additional fuzzing strategies**: New mutation patterns
- **Performance optimizations**: Faster simulation execution
- **Documentation**: Examples, tutorials, API docs
- **Test coverage**: Help us reach 80%+

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
