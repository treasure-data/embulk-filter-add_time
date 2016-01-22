package org.embulk.filter.add_time.converter;

import org.embulk.filter.add_time.reader.ColumnReader;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;

public class ColumnDuplicator
        implements ColumnConverter
{
    public static class Builder
    {
        private Column column;
        private ColumnReader duplicator;
        private ColumnReader duplicatee;

        public Builder()
        {
        }

        public Builder setColumn(Column column)
        {
            this.column = column;
            return this;
        }

        public Builder setDuplicator(ColumnReader duplicator)
        {
            this.duplicator = duplicator;
            return this;
        }

        public Builder setDuplicatee(ColumnReader duplicatee)
        {
            this.duplicatee = duplicatee;
            return this;
        }

        public ColumnDuplicator build()
        {
            return new ColumnDuplicator(column, duplicator, duplicatee);
        }
    }

    private final Column column;
    private final ColumnReader duplicator;
    private final ColumnReader duplicatee;

    private ColumnDuplicator(Column column, ColumnReader duplicator, ColumnReader duplicatee)
    {
        this.column = column;
        this.duplicator = duplicator;
        this.duplicatee = duplicatee;
    }

    public void update(PageReader pageReader)
    {
        duplicator.readValue(column, pageReader);
        duplicator.copyTo(duplicatee);
    }

    public void convert(PageBuilder pageBuilder)
    {
        duplicator.convertValue(column, pageBuilder);
    }

    public void addColumn(Schema.Builder schemaBuilder)
    {
        schemaBuilder.add(column.getName(), column.getType());
    }
}
