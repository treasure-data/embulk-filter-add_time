package org.embulk.filter.add_time.converter;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import org.embulk.filter.add_time.AddTimeFilterPlugin.FromColumnConfig;
import org.embulk.filter.add_time.AddTimeFilterPlugin.ToColumnConfig;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.util.timestamp.TimestampFormatter;

public class StringValueCastConverter
        extends ValueCastConverter
{
    private final TimestampFormatter fromTimestampFormatterForParsing;

    public StringValueCastConverter(FromColumnConfig fromColumnConfig, ToColumnConfig toColumnConfig)
    {
        super(toColumnConfig);
        final String pattern = fromColumnConfig.getFormat().orElse(fromColumnConfig.getDefaultTimestampFormat());
        this.fromTimestampFormatterForParsing = TimestampFormatter.builder(pattern, true)
                        .setDefaultZoneFromString(fromColumnConfig.getTimeZoneId().orElse(fromColumnConfig.getDefaultTimeZoneId()))
                        .setDefaultDateFromString(fromColumnConfig.getDate().orElse(fromColumnConfig.getDefaultDate()))
                        .build();
    }

    @Override
    public void convertValue(final Column column, String value, final PageBuilder pageBuilder)
    {
        columnVisitor.setValue(stringToInstant(value));
        columnVisitor.setPageBuilder(pageBuilder);
        column.visit(columnVisitor);
    }

    private Instant stringToInstant(final String value)
    {
        return this.fromTimestampFormatterForParsing.parse(value);
    }
}
