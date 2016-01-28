# Add time filter plugin for Embulk

The `embulk-filter-add_time` plugin allows users to add a new time-based column to the existing schema either by copying the value from another existing column in the schema or by specifying the value. The latter use case is particularly useful to tag all events bulk loaded with one Embulk run with a specific timestamp or time range.

## Overview

* **Plugin type**: filter

## Configuration

### to_column configuration

This configuration is **required**.

The `to_column` configuration specifies the name, type, and optional format of a new column to be added to the Embulk schema.

This configuration specifies the name, type, and format for the added time column as a set of key-value pairs:
- **name**<br/>
a string specifying the name of the new column (**required**).
- **type**<br/>
a string specifying the data type of the column: `long` or `timestamp` are the supported values for this parameter since they need to express a valid timestamp (**required**).
- **unix_timestamp_unit**<br/>
a string specifying the unit for the unix timestamp - it is required if the column type is `long`. `sec`, `milli` (for milliseconds), `micro` (for microseconds), or `nano` (for nanoseconds) are the supported values for this option (default is `sec`).

For example, the `to_column` configuration can be used to add a `'time'` column to the schema:

```yaml
filters:
- type: add_time
  to_column:
    name: time
    type: timestamp
...
```

Note that if a column with the **same name** as `name` already exists in the schema, the existing column name is changed by appending '\_' to its name. For example, if `'created_at'` is specified as `to_column` but the column name already exists, the existing column name is changed to `'created_at_'`.

When `long` type is specified for the `type` parameter, `unix_timestamp_unit` is required to convert from the unit timestamp value to `long` and tell the plugin how to exactly parse the timestamp number; `sec` for seconds, `milli` for milliseconds, `micro` for microseconds, or `nano` for nanoseconds are the supported values for this option:

```yaml
filters:
- type: add_time
  to_column:
    name: time
    type: long
    unix_timestamp_unit: sec
...
```

### from_value configuration

This configuration is mutually exclusive with `from_column` below. One amongst `from_column` and `from_value` configurations is required.

The `from_value` configuration specifies a specific value or range to be used as value of the column added by the `to_column` configuration. This configuration supports different modes and the behavior will vary accordingly. Refer to the examples below.

This configuration specifies the mode, the timestamp_format, and one amongst value, from and to, or nothing depending on the mode of choice as a set of key-value pairs.
These parameters are required:
- **mode**<br/>
a string specifying the mode. There are 3 valid modes (default is `fixed_time`):
  * `fixed_time`<br/>
  In this mode, all values of the added `to_column` column are set with the fixed value specified by the `value` parameter, which becomes a required parameter, see below;
  * `incremental_time`<br/>
  In this mode, all values of the added `to_column` column are set to a value that increments by 1 second for each record, starting at `from` timestamp and up to `to` timestamp, after which it wrap around and starts from `from` again and so on - hence the additional `from` and `to` parameters are required in this mode.
  * `upload_time`<br/>
  In this mode, all values of the added `to_column` column are set with the fixed value corresponding to the time the Embulk upload was started. This mode does not require additional parameters.
- **timestamp_format**<br/>
a string specifying how to parse the string value provided as either `value` or `from`/`to` parameters depending on the `mode` in use (default is `"%Y-%m-%d %H:%m:%s %Z"`).<br/>
It follow the [Ruby's `strptime` format](http://ruby-doc.org/stdlib-2.0.0/libdoc/date/rdoc/DateTime.html#method-c-strptime). Note that at the moment also Unix timestamps and epoch times need to be specified as string, therefore the `timestamp_format` parameter needs to specify how to parse it.

These parameters are mutually exclusive and the usage depend on the `mode`. The format of the value used here needs to match the rule provided in the `timestamp_format` parameter:
- **value**<br/>
a string specifying a fixed value for the added time column. This options is required if the mode is `fixed_time`.
- **from** and **to**<br/>
two strings specifying the value for the beginning and end of the range of time for `mode: incremental_time`. The format needs to be consistent between these two parameters.

Example: `mode: fixed_time`
```yaml
filters:
- type: add_time
  to_column:
    name: time
    type: timestamp
  from_value:
    mode: fixed_time
    value: "1453939479"
    timestamp_format: "%s"
```

The mode `fixed_time` is default and can be omitted:

```yaml
filters:
- type: add_time
  to_column:
    name: time
    type: timestamp
  from_value:
    value: "2016-01-01 00:00:00 UTC"
```

where in this example the format of the value is also corresponding to the default, therefore the `timestamp_format` option is also omitted.

Example: `mode: incremental_time`
```yaml
filters:
- type: add_time
  to_column:
    name: time
    type: timestamp
  from_value:
    mode: incremental_time
    from: "2016-01-01 00:00:00 UTC"
    to: "2016-01-01 01:00:00 UTC"
```

Example: `mode: upload_time`
```yaml
filters:
- type: add_time
  to_column:
    name: time
    type: timestamp
  from_value:
    mode: upload_time
```

where neither of the additional parameters `value`, `to`, and `from` is specified. Embulk will associate the fixed bulk upload start time to every record.

### from_column configuration

This configuration is mutually exclusive with `from_value` above. One amongst `from_column` and `from_value` configurations is required.

The `from_column` configuration specifies the name of one of the columns found in the Embulk schema and the format to be used to parse and feed values in the column added by the `to_column` configuration. This configuration makes a copy of the values from the column specified by `name`, instead of renaming the source column itself.
The parameters for this configuration are expressed as a set of key-value pairs.
- **name**<br/>
a string specifying the name of the source column from the Embulk schema. The column type must be one of `long`, `timestamp` or `string` (**required**).
- **unix_timestamp_unit**<br/>
a string specifying the expected unit of the unix timestamp for the values of the source column: it is **required** only if the type of the source column is `long` (see above for supported types). The supported values are `sec` for seconds, `milli` for milliseconds, `micro` for microseconds, and `nano` for nanoseconds (default is `sec`).
- **timestamp_format**<br/>
a string specifying the expected format of the values in the source column: it is **required** only if the type of the column is `string` (see above for supported types) (default is `"%Y-%m-%d %H:%M:%S %Z"`). It follow the [Ruby's `strptime` format](http://ruby-doc.org/stdlib-2.0.0/libdoc/date/rdoc/DateTime.html#method-c-strptime).

Note that if neither `timestamp_format` or `unix_timestamp_unit` parameters are specified, the column type is expected to be `timestamp`.

Example: `created_at` column with `timestamp` data type
```yaml
filters:
- type: add_time
  to_column:
    name: time
    type: timestamp
  from_column:
    name: created_at
```

In this example the expected type for the source column `created_at` is `timestamp`. If the `created_at` column type is string, the `timestamp_format` parameter is required to provide Embulk with a way to parse the values and convert them to a `timestamp`.

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

If the type of the source column is long, the `unix_timestamp_unit` parameter is required. The additional parameter provides Embulk the information to properly parse the `long` values of the source column and convert them to type specified in the `to_column` configuration`.

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
