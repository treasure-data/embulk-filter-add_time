package org.embulk.filter.add_time.reader;

import org.embulk.filter.add_time.converter.ValueConverter;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;

public class DoubleColumnReader
        extends AbstractColumnReader<DoubleColumnReader>
{
    protected double value;

    public DoubleColumnReader(ValueConverter valueConverter)
    {
        super(valueConverter);
    }

    @Override
    public void readNonNullValue(Column column, PageReader pageReader)
    {
        value = pageReader.getDouble(column);
    }

    @Override
    public void convertNonNullValue(Column column, PageBuilder pageBuilder)
    {
        valueConverter.convertValue(column, value, pageBuilder);
    }

    @Override
    public void copyValueTo(DoubleColumnReader columnReader)
    {
        columnReader.value = this.value;
    }
}
