# Formation evaluation importer

Parses LAS log files v1.2 and v2.0.

Corva supports viewing LAS logs files.
In order to bring LAS file to Corva
it needs to be parsed.
This app parses LAS files
and persists relevant data in the database.

## Requirements

* Installed `uv`.
* Installed `make`.

## Help
* Run `make help` to see the list of available commands.

## Developing

### Set up
* Run `make install`.

### Run tests and linter
* Static code analysis: `make lint`.
* Unit tests: `make test` or `make testcov`
  (later command will display code coverage in the browser).
  
