# Formation evaluation importer

Parses LAS log files v1.2 and v2.0.

Corva supports viewing LAS logs files. In order to bring LAS
file to Corva it needs to be parsed. This app parses LAS files
and persists relevant data in the database.

# Requirements

Python 3.8+

# Developing

#### Set up the project

```console
$ python -m venv venv
$ source venv/bin/activate
$ pip install -r dev-requirements.txt
```

#### Run code linter

```console
$ flake8
```