package org.embulk.filter.add_time.converter;

import org.embulk.filter.add_time.reader.ColumnReader;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;

public class SimpleColumnConverter
        implements ColumnConverter
{
    public static class Builder
    {
        private Column column;
        private ColumnReader reader;

        public Builder()
        {
        }

        public Builder setColumn(Column column)
        {
            this.column = column;
            return this;
        }

        public Builder setColumnReader(ColumnReader reader)
        {
            this.reader = reader;
            return this;
        }

        public ColumnConverter build()
        {
            return new SimpleColumnConverter(column, reader);
        }
    }

    private final Column column;
    private final ColumnReader columnReader;

    private SimpleColumnConverter(Column column, ColumnReader columnReader)
    {
        this.column = column;
        this.columnReader = columnReader;
    }

    public void update(PageReader pageReader)
    {
        columnReader.readValue(column, pageReader);
    }

    public void convert(PageBuilder pageBuilder)
    {
        columnReader.convertValue(column, pageBuilder);
    }

    public void addColumn(Schema.Builder schemaBuilder)
    {
        schemaBuilder.add(column.getName(), column.getType());
    }
}
