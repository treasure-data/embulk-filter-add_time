# Add time filter plugin for Embulk

Add time column to the schema

## Overview

* **Plugin type**: filter

## Configuration

- **option1**: description (integer, required)
- **option2**: description (string, default: `"myvalue"`)
- **option3**: description (string, default: `null`)

## Example

```yaml
filters:
  - type: add_time
    option1: example1
    option2: example2
```

## Install

```
$ embulk gem install embulk-filter-add_time
```

## Build

### Build by Gradle
```
$ git clone https://github.com/treasure-data/embulk-filter-add_time.git
$ cd embulk-filter-add_time
$ ./gradlew gem classpath
```

### Run on Embulk
$ bin/embulk run -I embulk-filter-add_time/lib/ config.yml
