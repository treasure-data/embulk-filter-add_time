package org.embulk.filter.add_time.converter;

import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;

public interface ColumnConverter
{
    void update(PageReader pageReader);

    void convert(PageBuilder pageBuilder);

    void addColumn(Schema.Builder schemaBuilder);
}
