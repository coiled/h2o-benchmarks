If you want to contribute to this repository, make sure you run the pre-commit linters before opening a Pull Request:

1. Install pre-commit in your environment:

```
pip install pre-commit
```

2. Before pushing your code, run the pre-commit

```
pre-commit run --all-files
```

**Note**

If you wish to setup the pre-commit hooks to run automatically when you make a git commit you, this can be done by running:

```
pre-commit install
```