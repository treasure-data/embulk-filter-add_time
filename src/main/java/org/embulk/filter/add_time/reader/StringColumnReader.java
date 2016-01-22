package org.embulk.filter.add_time.reader;

import org.embulk.filter.add_time.converter.ValueConverter;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;

public class StringColumnReader
        extends AbstractColumnReader<StringColumnReader>
{
    protected String value;

    public StringColumnReader(ValueConverter valueConverter)
    {
        super(valueConverter);
    }

    @Override
    public void readNonNullValue(Column column, PageReader pageReader)
    {
        value = pageReader.getString(column);
    }

    @Override
    public void convertNonNullValue(Column column, PageBuilder pageBuilder)
    {
        valueConverter.convertValue(column, value, pageBuilder);
    }

    @Override
    public void copyValueTo(StringColumnReader columnReader)
    {
        columnReader.value = this.value;
    }
}
