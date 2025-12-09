# OpenAQ Anomaly Prediction

Welcome to the **OpenAQ Anomaly Prediction** project: an end-to-end project to make daily anomaly predictions in the air quality using neural networks (auto-encoders) and time series from OpenAQ data.

<!-- This documentation contains the following sections:

- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Package Customization](#package-customization)
- [Development Workflow](#development-workflow)
- [Documentation](#documentation) -->

# Quick Start

## 1. Clone the repository

```bash
git clone <this-repo-url>
cd openaq-anomaly-prediction
```

## 2. Setup Development Environment

```bash
# Install dependencies and setup virtual environment
make setup
```

That's it! Your development environment is ready:
- New local pyenv environment: `openaq-anomaly-prediction`
- Dependencies managed with Poetry: `pyproject.toml`
- Dev dependencies: `ipykernel` and `ruff`


## 3. Add your API keys (development)

The project is relying on `direnv` to load API keys during development.
Make sure you have `direnv` installed globally in your shell.

You should add your own API keys in the `secrets/.env` file, corresponding to the example file `secrets/.env_example`.



