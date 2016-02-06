package org.embulk.filter.add_time.converter;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;

public class ValueNoConverter
        implements ValueConverter
{
    @Override
    public void convertNull(Column column, PageBuilder pageBuilder)
    {
        pageBuilder.setNull(column);
    }

    @Override
    public void convertValue(Column column, boolean value, PageBuilder pageBuilder)
    {
        pageBuilder.setBoolean(column, value);
    }

    @Override
    public void convertValue(Column column, long value, PageBuilder pageBuilder)
    {
        pageBuilder.setLong(column, value);
    }

    @Override
    public void convertValue(Column column, double value, PageBuilder pageBuilder)
    {
        pageBuilder.setDouble(column, value);
    }

    @Override
    public void convertValue(Column column, String value, PageBuilder pageBuilder)
    {
        pageBuilder.setString(column, value);
    }

    @Override
    public void convertValue(Column column, Value value, PageBuilder pageBuilder)
    {
        pageBuilder.setJson(column, value);
    }

    @Override
    public void convertValue(Column column, Timestamp value, PageBuilder pageBuilder)
    {
        pageBuilder.setTimestamp(column, value);

    }
}