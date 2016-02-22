package org.embulk.filter.add_time.converter;

import org.embulk.filter.add_time.AddTimeFilterPlugin.FromColumnConfig;
import org.embulk.filter.add_time.AddTimeFilterPlugin.ToColumnConfig;
import org.embulk.filter.add_time.AddTimeFilterPlugin.UnixTimestampUnit;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParseException;
import org.embulk.spi.time.TimestampParser;
import org.msgpack.core.MessagePackException;
import org.msgpack.value.Value;

import java.util.Map;

import static org.msgpack.value.ValueFactory.newString;

public class JsonValueCastConverter
        extends ValueCastConverter
{
    private final Value jsonKey;
    private final TimestampParser fromTimestampParser; // for string value
    private final UnixTimestampUnit fromUnixTimestampUnit; // for long value

    public JsonValueCastConverter(FromColumnConfig fromColumnConfig, ToColumnConfig toColumnConfig)
    {
        super(toColumnConfig);
        this.jsonKey = newString(fromColumnConfig.getJsonKey().get());
        this.fromTimestampParser = new TimestampParser(fromColumnConfig, fromColumnConfig);
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
                columnVisitor.setValue(stringToTimestamp(v));
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
        catch (InvalidCastException | TimestampParseException | MessagePackException e) {
            log.warn(String.format("Cannot convert (%s): %s", e.getMessage(), value.toJson()));
            pageBuilder.setNull(column);
        }
    }

    private Timestamp stringToTimestamp(Value value)
    {
        return fromTimestampParser.parse(value.asStringValue().toString());
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
