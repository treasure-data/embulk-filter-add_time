package org.embulk.filter.add_time.converter;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;

public interface ValueConverter
{
    ValueConverter NO_CONV = new ValueNoConverter();

    void convertNull(Column column, PageBuilder pageBuilder);

    void convertValue(Column column, boolean value, PageBuilder pageBuilder);

    void convertValue(Column column, long value, PageBuilder pageBuilder);

    void convertValue(Column column, double value, PageBuilder pageBuilder);

    void convertValue(Column column, String value, PageBuilder pageBuilder);

    void convertValue(Column column, Value value, PageBuilder pageBuilder);

    void convertValue(Column column, Timestamp value, PageBuilder pageBuilder);
}
