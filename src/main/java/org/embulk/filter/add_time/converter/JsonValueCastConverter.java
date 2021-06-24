package org.embulk.filter.add_time.converter;

import org.embulk.filter.add_time.AddTimeFilterPlugin.FromColumnConfig;
import org.embulk.filter.add_time.AddTimeFilterPlugin.ToColumnConfig;
import org.embulk.filter.add_time.AddTimeFilterPlugin.UnixTimestampUnit;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.core.MessagePackException;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;

public class JsonValueCastConverter
        extends ValueCastConverter
{
    private final Value jsonKey;
    private final TimestampFormatter fromTimestampFormatterForParsing; // for string value
    private final UnixTimestampUnit fromUnixTimestampUnit; // for long value

    public JsonValueCastConverter(FromColumnConfig fromColumnConfig, ToColumnConfig toColumnConfig)
    {
        super(toColumnConfig);
        this.jsonKey = ValueFactory.newString(fromColumnConfig.getJsonKey().get());

        final String pattern = fromColumnConfig.getFormat().orElse(fromColumnConfig.getDefaultTimestampFormat());
        this.fromTimestampFormatterForParsing = TimestampFormatter.builder(pattern, true)
                        .setDefaultZoneFromString(fromColumnConfig.getTimeZoneId().orElse(fromColumnConfig.getDefaultTimeZoneId()))
                        .setDefaultDateFromString(fromColumnConfig.getDate().orElse(fromColumnConfig.getDefaultDate()))
                        .build();

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
                columnVisitor.setValue(stringToInstant(v));
            }
            else if (v.isIntegerValue()) {
                columnVisitor.setValue(longToInstant(v));
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
        return fromTimestampFormatterForParsing.parse(value.asStringValue().toString());
    }

    private Instant longToInstant(Value value)
    {
        return fromUnixTimestampUnit.toInstant(value.asIntegerValue().toLong());
    }

    static class InvalidCastException
            extends RuntimeException
    {
        InvalidCastException(String message) {
            super(message);
        }
    }
}
