# Crunch

[![Build Status](https://travis-ci.org/Shelnutt2/crunch.svg?branch=master)](https://travis-ci.org/Shelnutt2/crunch)

MariaDB storage engine based on cap'n proto storage

## Requirements

Requires MariaDB 10.1 or newer. It is untested on older versions.

## Installation

There are two methods to building crunch.

### Inside MariaDB Source Tree (Recommended)
The first is inside a MariaDB source tree (recommended).

```bash
git clone git@github.com:MariaDB/server.git
cd server
git submodule add git@github.com:Shelnutt2/crunch.git storage/crunch
mkdir build && cd build
cmake ..
make -j4
```

### Standalone compilation

The second method is building it standalone and
installing the library to the mariadb plugin folder.
This method is not recommended, as the compiler and
compiler flags must match for mariadb to load the plugin.

```bash
git clone git@github.com:Shelnutt2/crunch.git
cd crunch
mkdir build && cd build
cmake .. -DCRUNCH_COMPILE_STANDALONE=ON
make -j4
```
