# DuckDB DNS Query extension
This repository contains extension for duckdb which allows to query dns records similiarly as one can do with dig on command line:
```sh
$ dig -t cname +short app.reddit.org
pltraffic7.com.
$ dig -t cname +short flights.google.com
www3.l.google.com.
```

## Building extension on MacOS
Install requirements:
```
$ brew install pkgconf ninja autoconf automake
$ git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
```

Ensure that git submodules have been checkout:
```
$ git submodule update --init --remote --recursive
```

Build the project using ninja and vckpg:
```
$ make release GEN=ninja VCPKG_TOOLCHAIN_PATH=$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake
```

## Caching
Don't remove the `build` directory everytime after changes. This will make it much more slower to develop

## Testing
**IMPORTANT: After the build is finished you need to run: `make test`**

This will run tests in the `tests/` folder.