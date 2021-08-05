# Drilling WITS Depth Median Backend App

**Drilling WITS Depth Median** is a depth scheduled app 
that calculates and stores `drilling.wits.depth` data __medians__.

## Packaging
To package the app for deployment to Dev Center
use following command, which will output `package.zip`:
```bash
PROVIDER=my-provider make build
```

## Contributing

What's needed:
* Linux with `make` installed.
* Python 3.8.
* Opened terminal inside cloned repository.

Run following commands:
```bash
1 $ python -m venv env
2 $ source ./env/bin/activate
3 $ make install
4 $ make lint test
```
1. Create an isolated Python environment.
2. Activate the new environment.
3. Install all requirements.
4. Run linter and tests
to verify that the project was set up properly.

### What's next?
After completing steps above
you can explore the project
and make a contribution.
Run `make help`
to see the list of available commands.
