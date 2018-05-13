# How to use

## Compile and run the main class of a certain implementation:
```bash
make compile class=<implementation package name>
```
Example:
```bash
make compile class=streaming
```

## Running test (for theta join implementation only):
```bash
make test
```

## Build java jar package:
```bash
make package
```

## Copying the jar into docker dir
```bash
make copy
```
Then proceed into `docker` dir.
