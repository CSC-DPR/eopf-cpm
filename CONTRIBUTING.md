# Install your environment

## Conda developer

If you are using conda, to setup the development environment just load it from
the environment.yaml:
```bash
conda env create -f environment.yaml
```

you will be able to access to a `eopf-dev` conda environment.


## Pipenv developer

If you are using pipenv, to setup the development environment just load it from
the Pipfile:
```bash
pipenv install --dev
```

to access to the environment, once you are on the correct folder, just run:
```bash
pipenv shell
```

or if you just want to run a command inside:
```bash
pipenv run my_cmd args
```

## Install eopf as development package

Once your environment is setup, to install eopf as development package, run
the pip install command:
```bash
pip install -e path/to/eopf/folder
```

> **WARNING** If pip is not up to date, you can have an error, so, try `pip install -U pip` before.


## Pre-commit hook

We use  [pre-commit](https://pre-commit.com/) to reduce linting and formatting fix at Pull request time, some hooks are define in .pre-commit-config.yaml
To install [pre-commit](https://pre-commit.com/), after you have install and activate your python environment, run:
`pre-commit install`

to use pre-commit out of the git hooks context, run:
`pre-commit run <hook-id>`
or
`pre-commit run --all-files`

# TESTING

Test are writed using [pytest](https://docs.pytest.org/en/7.0.x/).
To run all the test, be sure to have the needed data.

Also, we have some mark to help to contegorized test and only run unit test at integration time.

- to mark use case test integration, please mark them with ```@pytest.mark.usecase```
- to mark unit test integration, please mark them with ```@pytest.mark.unit```

# Linting and formatting

please refer to pre-commit hooks

# Method / Function order

We try to follow the following scope order to have harmonized structure:
- Global attributes
- `__init__` (for classes)
- magic dunders methods (for classes)
- public property/methods/function
- protected property/methods/function
- private property/methods/function
- public staticmethod and classmethod (for classes)
- public staticmethod and classmethod (for classes)

Each scope follow the alphabetic order

# Git Commit Convention

For commits, we use the [Conventional commits specification](https://www.conventionalcommits.org/en/v1.0.0/)

## Type

Must be one of the following:

- build: Changes that affect the build system or external dependencies (example scopes: gulp, broccoli, npm)
- ci: Changes to our CI configuration files and scripts (example scopes: Travis, Circle, BrowserStack, SauceLabs)
- docs: Documentation only changes
- feat: A new feature
- fix: A bug fix
- perf: A code change that improves performance
- refactor: A code change that neither fixes a bug nor adds a feature
- style: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
- test: Adding missing tests or correcting existing tests
