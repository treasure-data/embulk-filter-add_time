package org.embulk.filter.add_time.reader;

import org.embulk.filter.add_time.converter.ValueConverter;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;

public class BooleanColumnReader
        extends AbstractColumnReader<BooleanColumnReader>
{
    protected boolean value;

    public BooleanColumnReader(ValueConverter valueConverter)
    {
        super(valueConverter);
    }

    @Override
    public void readNonNullValue(Column column, PageReader pageReader)
    {
        value = pageReader.getBoolean(column);
    }

    @Override
    public void convertNonNullValue(Column column, PageBuilder pageBuilder)
    {
        valueConverter.convertValue(column, value, pageBuilder);
    }

    @Override
    public void copyValueTo(BooleanColumnReader columnReader)
    {
        columnReader.value = this.value;
    }
}
