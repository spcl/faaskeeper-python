# FaaSKeeper Python

**The Python client library for the serverless coordination service FaaSKeeper.**

[![GH Actions](https://github.com/mcopik/faaskeeper-python/actions/workflows/build.yml/badge.svg)](https://github.com/mcopik/faaskeeper-python/actions/workflows/build.yml)

The client library used by Python applications to connect to a FaaSKeeper instance.
At the moment we support an AWS deployment only. Adding support for other commercial clouds (Azure, GCP)
is planned in the future.

## Installation

Please run `pip install -e .` to install the library.

In future, we are going to provide a PyPI package.

## Usage

To use FaaSKeeper, you need to create a client by specyfing the cloud (only `aws` at the moment),
the name of the service specified when deploying FaaSKeeper to the cloud, and the port that
we can use to listen for incoming messages.

```
from faaskeeper.client import FaaSKeeperClient

try:
    client = FaaSKeeperClient("aws", "faaskeeper-dev", port=13001)
    client.start()
    ret = client.create("/root/test2", b"test")
    ret2 = client.get_data("/root/test2")
    print("Data stored in FK:", ret2)
finally:
    client.stop()
```

Please take a look at `examples` to see how `faaskeeper` interface can be used.

## Development

We use `black` and `flake8` for code linting. Before commiting and pushing changes,
please run `tools/linting.py faaskeeper` to verify that there are no issues with your code.

We use Python type hints ([PEP](https://www.python.org/dev/peps/pep-0484/), [docs](https://docs.python.org/3/library/typing.html))
to enhance the readability of our code. When adding new interfaces and types, please use type hints.
The linting helper `tools/linting.py` includes a call to `mypy` to check for static typing errors.

To install the local development environment with all necessary packages, please use the `install.py`
script. The script takes one optional argument `--venv` with a path to the Python virtual environment,
and the default path is `python-venv`. Use `source {venv-path}/bin/activate` to use it.


