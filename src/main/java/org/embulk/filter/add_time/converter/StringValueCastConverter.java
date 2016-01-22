package org.embulk.filter.add_time.converter;

import org.embulk.filter.add_time.AddTimeFilterPlugin.FromColumnConfig;
import org.embulk.filter.add_time.AddTimeFilterPlugin.ToColumnConfig;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;

public class StringValueCastConverter
        extends ValueCastConverter
{
    private final TimestampParser fromTimestampParser;

    public StringValueCastConverter(FromColumnConfig fromColumnConfig, ToColumnConfig toColumnConfig)
    {
        super(toColumnConfig);
        this.fromTimestampParser = new TimestampParser(fromColumnConfig, fromColumnConfig);
    }

    @Override
    public void convertValue(final Column column, String value, final PageBuilder pageBuilder)
    {
        columnVisitor.setValue(stringToTimestamp(value));
        columnVisitor.setPageBuilder(pageBuilder);
        column.visit(columnVisitor);
    }

    private Timestamp stringToTimestamp(String value)
    {
        return fromTimestampParser.parse(value);
    }
}
