package org.embulk.filter.add_time.converter;

import java.time.Instant;
import org.embulk.filter.add_time.AddTimeFilterPlugin.ToColumnConfig;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public class TimestampValueCastConverter
        extends ValueCastConverter
{
    public TimestampValueCastConverter(ToColumnConfig toColumnConfig)
    {
        super(toColumnConfig);
    }

    @Override
    public void convertValue(Column column, final Instant value, final PageBuilder pageBuilder)
    {
        columnVisitor.setValue(value); // it can use the value directly.
        columnVisitor.setPageBuilder(pageBuilder);
        column.visit(columnVisitor);
    }
}
