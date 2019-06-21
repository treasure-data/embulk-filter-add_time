package org.embulk.filter.add_time.converter;

import java.time.Instant;
import org.embulk.filter.add_time.AddTimeFilterPlugin.FromColumnConfig;
import org.embulk.filter.add_time.AddTimeFilterPlugin.ToColumnConfig;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.util.timestamp.TimestampFormatter;

public class StringValueCastConverter
        extends ValueCastConverter
{
    private final TimestampFormatter fromTimestampFormatter;

    public StringValueCastConverter(FromColumnConfig fromColumnConfig, ToColumnConfig toColumnConfig)
    {
        super(toColumnConfig);
        final TimestampFormatter.Builder builder = TimestampFormatter.builder(
                fromColumnConfig.getFormat().orElse(fromColumnConfig.getDefaultTimestampFormat()), true);
        builder.setDefaultZoneFromString(fromColumnConfig.getTimeZoneId().orElse(fromColumnConfig.getDefaultTimeZoneId()));
        builder.setDefaultDateFromString(fromColumnConfig.getDate().orElse(fromColumnConfig.getDefaultDate()));
        this.fromTimestampFormatter = builder.build();
    }

    @Override
    public void convertValue(final Column column, String value, final PageBuilder pageBuilder)
    {
        columnVisitor.setValue(Timestamp.ofInstant(stringToInstant(value)));
        columnVisitor.setPageBuilder(pageBuilder);
        column.visit(columnVisitor);
    }

    private Instant stringToInstant(String value)
    {
        return fromTimestampFormatter.parse(value);
    }
}
