package org.embulk.filter.add_time.converter;

import static org.msgpack.value.ValueFactory.newString;

import java.time.format.DateTimeParseException;
import java.time.Instant;
import java.util.Map;
import org.embulk.filter.add_time.AddTimeFilterPlugin.FromColumnConfig;
import org.embulk.filter.add_time.AddTimeFilterPlugin.ToColumnConfig;
import org.embulk.filter.add_time.AddTimeFilterPlugin.UnixTimestampUnit;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.core.MessagePackException;
import org.msgpack.value.Value;

public class JsonValueCastConverter
        extends ValueCastConverter
{
    private final Value jsonKey;
    private final TimestampFormatter fromTimestampFormatter; // for string value
    private final UnixTimestampUnit fromUnixTimestampUnit; // for long value

    public JsonValueCastConverter(FromColumnConfig fromColumnConfig, ToColumnConfig toColumnConfig)
    {
        super(toColumnConfig);
        this.jsonKey = newString(fromColumnConfig.getJsonKey().get());

        final TimestampFormatter.Builder builder = TimestampFormatter.builder(
                fromColumnConfig.getFormat().orElse(fromColumnConfig.getDefaultTimestampFormat()), true);
        builder.setDefaultZoneFromString(fromColumnConfig.getTimeZoneId().orElse(fromColumnConfig.getDefaultTimeZoneId()));
        builder.setDefaultDateFromString(fromColumnConfig.getDate().orElse(fromColumnConfig.getDefaultDate()));
        this.fromTimestampFormatter = builder.build();

        this.fromUnixTimestampUnit = UnixTimestampUnit.of(fromColumnConfig.getUnixTimestampUnit());
    }

    @Override
    public void convertValue(final Column column, Value value, final PageBuilder pageBuilder)
    {
        try {
            if (!value.isMapValue()) {
                throw new InvalidCastException("The value must be map object.");
            }

            Map<Value, Value> map = value.asMapValue().map();
            if (!map.containsKey(jsonKey)) {
                throw new InvalidCastException("This record doesn't have a key specified as json_key.");
            }

            Value v = map.get(jsonKey);
            if (v.isStringValue()) {
                columnVisitor.setValue(Timestamp.ofInstant(stringToInstant(v)));
            }
            else if (v.isIntegerValue()) {
                columnVisitor.setValue(longToTimestamp(v));
            }
            else {
                throw new InvalidCastException(String.format(
                        "The value of a key specified as json_key must be long or string type. But it's %s", value.getValueType().name()));
            }

            columnVisitor.setPageBuilder(pageBuilder);
            column.visit(columnVisitor);
        }
        catch (InvalidCastException | DateTimeParseException | MessagePackException e) {
            log.warn(String.format("Cannot convert (%s): %s", e.getMessage(), value.toJson()));
            pageBuilder.setNull(column);
        }
    }

    private Instant stringToInstant(Value value)
    {
        return fromTimestampFormatter.parse(value.asStringValue().toString());
    }

    private Timestamp longToTimestamp(Value value)
    {
        return fromUnixTimestampUnit.toTimestamp(value.asIntegerValue().toLong());
    }

    static class InvalidCastException
            extends RuntimeException
    {
        InvalidCastException(String message) {
            super(message);
        }
    }
}
