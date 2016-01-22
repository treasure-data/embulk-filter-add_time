package org.embulk.filter.add_time.converter;

import org.embulk.filter.add_time.AddTimeFilterPlugin.ToColumnConfig;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;

public class TimestampValueCastConverter
        extends ValueCastConverter
{
    public TimestampValueCastConverter(ToColumnConfig toColumnConfig)
    {
        super(toColumnConfig);
    }

    @Override
    public void convertValue(Column column, final Timestamp value, final PageBuilder pageBuilder)
    {
        columnVisitor.setValue(value); // it can use the value directly.
        columnVisitor.setPageBuilder(pageBuilder);
        column.visit(columnVisitor);
    }
}
