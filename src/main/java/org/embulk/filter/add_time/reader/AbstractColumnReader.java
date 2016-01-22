package org.embulk.filter.add_time.reader;

import org.embulk.filter.add_time.converter.ValueConverter;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;

public abstract class AbstractColumnReader<T extends AbstractColumnReader>
        implements ColumnReader<T>
{
    protected final ValueConverter valueConverter;
    protected boolean isNull = false;

    protected AbstractColumnReader(ValueConverter valueConverter)
    {
        this.valueConverter = valueConverter;
    }

    @Override
    public void readValue(Column column, PageReader pageReader)
    {
        if (!(isNull = pageReader.isNull(column))) {
            readNonNullValue(column, pageReader);
        }
    }

    protected abstract void readNonNullValue(Column column, PageReader pageReader);

    @Override
    public void convertValue(Column column, PageBuilder pageBuilder)
    {
        try {
            if (isNull) {
                valueConverter.convertNull(column, pageBuilder);
            }
            else {
                convertNonNullValue(column, pageBuilder);
            }
        }
        finally {
            isNull = false;
        }
    }

    protected abstract void convertNonNullValue(Column column, PageBuilder pageBuilder);

    @Override
    public void copyTo(T columnReader)
    {
        columnReader.isNull = this.isNull;
        copyValueTo(columnReader);
    }

    protected abstract void copyValueTo(T columnReader);
}
