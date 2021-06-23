package org.embulk.filter.add_time.reader;

import java.time.Instant;
import org.embulk.filter.add_time.converter.ValueConverter;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;

public class TimestampColumnReader
        extends AbstractColumnReader<TimestampColumnReader>
{
    protected Instant value;

    public TimestampColumnReader(ValueConverter valueConverter)
    {
        super(valueConverter);
    }

    @SuppressWarnings("deprecation")  // For use of PageReader#getTimestamp
    @Override
    public void readNonNullValue(Column column, PageReader pageReader)
    {
        value = pageReader.getTimestamp(column).getInstant();
    }

    @Override
    public void convertNonNullValue(Column column, PageBuilder pageBuilder)
    {
        valueConverter.convertValue(column, value, pageBuilder);
    }

    @Override
    public void copyValueTo(TimestampColumnReader columnReader)
    {
        columnReader.value = this.value;
    }
}
