package org.embulk.filter.add_time.reader;

import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;

public interface ColumnReader<T extends ColumnReader>
{
    void readValue(Column column, PageReader pageReader);

    void convertValue(Column column, PageBuilder pageBuilder);

    void copyTo(T columnReader);
}
