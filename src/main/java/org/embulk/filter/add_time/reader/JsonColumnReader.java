package org.embulk.filter.add_time.reader;

import org.embulk.filter.add_time.converter.ValueConverter;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.msgpack.value.Value;

public class JsonColumnReader
        extends AbstractColumnReader<JsonColumnReader>
{
    protected Value value;

    public JsonColumnReader(ValueConverter valueConverter)
    {
        super(valueConverter);
    }

    @Override
    public void readNonNullValue(Column column, PageReader pageReader)
    {
        value = pageReader.getJson(column);
    }

    @Override
    public void convertNonNullValue(Column column, PageBuilder pageBuilder)
    {
        valueConverter.convertValue(column, value, pageBuilder);
    }

    @Override
    public void copyValueTo(JsonColumnReader columnReader)
    {
        columnReader.value = this.value;
    }
}
