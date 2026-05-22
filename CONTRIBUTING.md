# Contributing to Consist

Thanks for your interest in contributing! Consist is currently maintained by a [small team](https://seta.lbl.gov/innovative-transportation-systems) at LBNL, but contributions of all kinds are welcome.

## Ways to Contribute

**Report a bug or request a feature:** Open an [issue](https://github.com/LBNL-UCB-STI/consist/issues). Please include enough context to reproduce the problem or understand the use case.

**Submit a pull request:** Fork the repo, create a branch, and open a PR. For anything beyond a small fix, it's worth opening an issue first so we can discuss the approach before you invest significant time.

**Ask a question:** If something in the docs is unclear or you're unsure whether Consist fits your use case, feel free to open an issue or email me directly at zaneedell@lbl.gov.

## Development Setup

```bash
# Clone and set up
git clone https://github.com/LBNL-UCB-STI/consist.git
cd consist

# With uv (recommended)
uv sync --group dev
uv run pytest

# Or with pip
pip install -e ".[dev]"
pytest
```

## Guidelines

- Please include tests for new functionality.
- Run the existing test suite before submitting a PR to make sure nothing is broken.
- Code is formatted and linted with [Ruff](https://docs.astral.sh/ruff/) — CI will check this automatically. You can run `ruff check` and `ruff format` locally before pushing. Type annotations are strongly encouraged, and the codebase is checked with [ty](https://github.com/astral-sh/ty).

## Code of Conduct

Be kind and constructive. This is a small project and contributions are typically reviewed by one person, so patience is appreciated. I'll do my best to respond to issues and PRs within a couple of weeks.

## License

By contributing, you agree that your contributions will be licensed under the [BSD 3-Clause License](LICENSE).