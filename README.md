# Panda Box
A simple library for fetching data from a Postgres / Redshift database into a Pandas DataFrame 
and running simple sequential transformation steps.

## Getting Started
Panda Box is available for installation via PyPi and pip.
```
pip install pandabox
```

## Running Tests and Lint
Unit tests can be run via the makefile

```
make unit_test
```

The flake 8 linter can also be run 

```
make lint
```

A convinience method to run both the unit tests and linter can be called

```
make qa
```
