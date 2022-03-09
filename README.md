# python-template

This is a template project, to make starting new Python projects easier. 
It implements a single package, called "novus" (latin for new). You should be able to
find/replace all instances of "novus" with your desired package name.

This template is somewhat opinionated, so feel free to remove/change things as you see fit.


### Pre-commit

To use pre-commit, first [install](https://pre-commit.com/#installation) it on your 
host machine. Note: it's too complex to put docker in charge of pre-commit.

```shell
pre-commit install  # installs the pre-commit hook for this repo
pre-commit run --all-files  # check all files
```

Check out `.pre-commit-config.yaml` to see the recommended basic pre-commit setup.

In general, it assumes you want to...
* use the black formatter (black).
* keep imports tidy (autoflake) and sorted (isort).
* prevent merge conflicts (check-merge-conflict).
* keep `setup.cfg` well-formatted (setup-cfg-fmt).
* follow pep-8 (flake8)

You may want to change some of these, or update the plugins to later versions.


### Docker

If you prefer using docker over virtual environments, see [DOCKER.md](./DOCKER.md)
instead of following the "Developer install" steps.


### Developer install

After setting up a virtual environment, run:
```shell
pip install -e ".[dev]"
pytest .
```

If you are able to run tests, then you should be set for development.

After the install, you should also be able to run `novus` and see the "hello world" message.


### Pycharm

Initial setup:
1. Right click on the `src` directory > Mark Directory As > Sources Root. The folder icon should turn blue.

Configure Python interpreter:
1. Go to Preferences/Settings > Project: python-template > python interpreter
2. Use the gear icon in the upper right to add your virtual environment or docker image to the project.

Configure the test runner & docstring format:
1. Go to Preferences/Settings > tools > Python integrated tools
2. Under "Testing" select `pytest` as the runner.
3. Under "Docstrings" select `Google` as the docstring format.

Set up linting keyboard shortcut (optional):
1. Preferences -> Tools -> External Tools > + (add new entry)
```
Name: lint
Description: Format the current file
Program: $PyInterpreterDirectory$/pre-commit
Arguments: run --files=$FilePath$Working 
Working Directory: $ProjectFileDir$
```
2. Preferences -> Keymap -> External Tools -> lint, 
   Assign the keyboard shortcut `Option-cmd-l`

### Type checking (mypy)

We recommend using mypy to check for type errors before committing. Simply run:
```shell
mypy src
```

Note: while there is a mypy plugin for pre-commit, I found it buggy and not worthwhile.

### Best practices
* Use type annotations on all functions and methods
* Write docstrings for everything, and use Google Docstring style
* Write unit tests for all major functionality
* If you are going to add a CLI, consider using typer (or argparse).
