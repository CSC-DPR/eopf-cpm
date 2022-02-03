# eopf-cpm

## Installation

### Conda developer

If you are using conda, to setup the development environment just load it from
the environment.yaml:
```bash
conda env create -f environment.yaml
```

you will be able to access to a `eopf-dev` conda environment.


### Pipenv developer

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

### Install eopf as development package

Once your environment is setup, to install eopf as development package, run
the pip install command:
```bash
pip install -e path/to/eopf/folder
```

> **WARNING** If pip is not up to date, you can have an error, so, try `pip install -U pip` before.
