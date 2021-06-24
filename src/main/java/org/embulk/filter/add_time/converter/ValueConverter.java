package org.embulk.filter.add_time.converter;

import java.time.Instant;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
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

    void convertValue(Column column, Instant value, PageBuilder pageBuilder);
}
