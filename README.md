# Installation

This package has only been tested on macOS.

```console
$ brew install uv ghostscript tcl-tk python-tk@3.12
```

## Dependencies

For more information on these dependencies installed with `brew`, see:

* [uv](https://github.com/astral-sh/uv/blob/main/README.md)
* [Camelot](https://camelot-py.readthedocs.io/en/master/user/install-deps.html)

# Run

```console
$ uv run --python 3.12.7 python -m main
```

Note that the same version of Python (3.12) must be used for running `uv` as was used to install `python-tk` with `brew`.