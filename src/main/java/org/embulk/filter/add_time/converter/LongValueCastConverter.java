package org.embulk.filter.add_time.converter;

import java.time.Instant;
import org.embulk.filter.add_time.AddTimeFilterPlugin.FromColumnConfig;
import org.embulk.filter.add_time.AddTimeFilterPlugin.ToColumnConfig;
import org.embulk.filter.add_time.AddTimeFilterPlugin.UnixTimestampUnit;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public class LongValueCastConverter
        extends ValueCastConverter
{
    private final UnixTimestampUnit fromUnixTimestampUnit;

    public LongValueCastConverter(FromColumnConfig fromColumnConfig, ToColumnConfig toColumnConfig)
    {
        super(toColumnConfig);
        this.fromUnixTimestampUnit = UnixTimestampUnit.of(fromColumnConfig.getUnixTimestampUnit());
    }

    @Override
    public void convertValue(final Column column, long value, final PageBuilder pageBuilder)
    {
        columnVisitor.setValue(longToInstant(value));
        columnVisitor.setPageBuilder(pageBuilder);
        column.visit(columnVisitor);
    }

    private Instant longToInstant(long value)
    {
        return fromUnixTimestampUnit.toInstant(value);
    }
}
