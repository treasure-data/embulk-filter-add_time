# Add time filter plugin for Embulk

Add time column to the schema

## Overview

* **Plugin type**: filter

## Configuration

- **to_column**: key-value pairs of options for new column
  - **name** name of a column. (string, required)
  - **type** type of a column. (string, required)
  - **unix_timestamp_unit** unit of the unix timestamp if type of the column is long. "sec", "milli" (for milliseconds), "micro" (for microseconds), or "nano" (for nanoseconds). (string, required if the type is long.)
- **from_column** key-value pairs of options for existing column
  - **name** name of the column. Its column type must be long, timestamp or string (string, required)
  - **unix_timestamp_unit** unit of the unix timestamp if type of the column is long. "sec", "milli" (for milliseconds), "micro" (for microseconds), or "nano" (for nanoseconds). (enum, default: `"sec"`)
  - **timestamp_format** if the embulk type is `string`, the column value is parsed by this timestamp_format. (string, default: `"%Y-%m-%d %H:%m:%s %Z"`)
- **from_value** key-value pairs of options for new column
  - **mode** mode of new column creation. "fixed_time", "incremental_time", or "upload_time". (enum, default: `"fixed_time"`)
  - **value** value of the fixed time if mode is "fixed_time". e.g. "2016-01-01 01:01:01 UTC". (string, required if "fixed_time" mode)
  - **from** value of the begin of the incremental time if mode is "incremental_time". e.g. "2016-01-01 00:00:00 UTC". (string, required if "incremental_time" mode)
  - **to** value of the end of the incremental time if mode is "incremental_time". e.g. "2016-01-02 00:00:00 UTC". (string, required if "incremental_time" mode)
  - **timestamp_format** if the embulk type is `string`, the column value is parsed by this timestamp_format. (string, default: `"%Y-%m-%d %H:%m:%s %Z"`)

## Example

embulk-filter-add_time plugin allows users to add new time based column to the existing schema.

### to_column option

`to_column` option allows add new column has the name specified as `name` and the type specified as `type`. For example, it adds new 'time' named column specified as `to_column` to the existing schema and inserts `Timestamp` value into each the column. If the column of same name specified as `to_column`.`name` already exists in the schema, the existing column name will be changed.

```yaml
filters:
- type: add_time
  to_column:
    name: time
    type: timestamp
  from_value:
    value: "2016-01-01 00:00:00 UTC"
```

When `long` type is specified as `to_column`.`type`, `unix_timestamp_unit` is required to convert from `Timestamp` value to `long` value.

```
filters:
- type: add_time
  to_column:
    name: time
    type: long
    unix_timestamp_unit: sec # "sec", "milli", "micro", or "nano"
  from_value:
    value: "2016-01-01 00:00:00 UTC"
```

### from_value option

`from_value` allows to insert the value specified as the option into each column created by `to_column`. An user can insert the value corresponding to the fixed value specified as `value`.

```yaml
filters:
- type: add_time
  to_column:
    name: time
    type: timestamp
  from_value:
    value: "2016-01-01 00:00:00 UTC"
```

`from_value` has 3 types of `mode`: `fixed_time` (default), `incremental_time`, and `upload_time`. `fixed_time` mode allows inserting a fixed (constant) value into the column. It's default and can be omitted.

```yaml
filters:
- type: add_time
  to_column:
    name: time
    type: timestamp
  from_value:
    mode: fixed_time
    value: "2016-01-01 00:00:00 UTC"
```

`incremental_time` mode can insert incremental timestamps to the column. Timestamp values will be incremented by 1 second each value. `upload_time` mode can insert Embulk's constant transaction_time to the column.

### from_column option

`from_column` option allows to copy values from the existing `Timestamp` typed column specified as `from_column` to new column specified as `to_column` as following.

```yaml
filters:
- type: add_time
  to_column:
    name: time
    type: timestamp
  from_column:
    name: created_at
```

When `created_at` column is string, `timestamp_format` option is required because the string value should convert to `Timestamp` value.

```yaml
filters:
- type: add_time
  to_column:
    name: time
    type: timestamp
  from_column:
    name: created_at
    timestamp_format: "%Y-%m-%d %H:%M:%S"
    timezone: UTC
```

When the column is long, `unix_timestamp_unit` option is required because of long value convertion.

```yaml
filters:
- type: add_time
  to_column:
    name: time
    type: long
    unix_timestamp_unit: sec
  from_column:
    name: created_at
    unixtime_unit: milli
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
