package org.embulk.filter.add_time.converter;

import org.embulk.filter.add_time.AddTimeFilterPlugin.ToColumnConfig;
import org.embulk.filter.add_time.AddTimeFilterPlugin.UnixTimestampUnit;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;

public abstract class ValueCastConverter
        implements ValueConverter
{
    protected final TimestampValueCastVisitor columnVisitor;

    public ValueCastConverter(ToColumnConfig toColumnConfig)
    {
        this.columnVisitor = new TimestampValueCastVisitor(UnixTimestampUnit.of(toColumnConfig.getUnixTimestampUnit()));
    }

    @Override
    public void convertNull(Column column, PageBuilder pageBuilder)
    {
        pageBuilder.setNull(column);
    }

    @Override
    public void convertValue(Column column, boolean value, PageBuilder pageBuilder)
    {
        throw new AssertionError("Never call.");
    }

    @Override
    public void convertValue(Column column, long value, PageBuilder pageBuilder)
    {
        throw new AssertionError("Should implement in subclass.");
    }

    @Override
    public void convertValue(Column column, double value, PageBuilder pageBuilder)
    {
        throw new AssertionError("Never call.");
    }

    @Override
    public void convertValue(final Column column, String value, final PageBuilder pageBuilder)
    {
        throw new AssertionError("Should implement in subclass.");
    }

    @Override
    public void convertValue(final Column column, Value value, final PageBuilder pageBuilder)
    {
        throw new AssertionError("Should implement in subclass.");
    }

    @Override
    public void convertValue(Column column, final Timestamp value, final PageBuilder pageBuilder)
    {
        throw new AssertionError("Never call.");
    }

    static class TimestampValueCastVisitor
            implements ColumnVisitor
    {
        private final UnixTimestampUnit toUnixTimestampUnit;
        private PageBuilder currentPageBuilder;
        private Timestamp currentValue;

        TimestampValueCastVisitor(UnixTimestampUnit toUnixTimestampUnit)
        {
            this.toUnixTimestampUnit = toUnixTimestampUnit;
        }

        void setValue(Timestamp value)
        {
            this.currentValue = value;
        }

        void setPageBuilder(PageBuilder pageBuilder)
        {
            this.currentPageBuilder = pageBuilder;
        }

        @Override
        public void booleanColumn(Column column)
        {
            throw new AssertionError("Never call.");
        }

        @Override
        public void longColumn(Column column)
        {
            currentPageBuilder.setLong(column, toUnixTimestampUnit.toLong(currentValue));
        }

        @Override
        public void doubleColumn(Column column)
        {
            throw new AssertionError("Never call.");
        }

        @Override
        public void stringColumn(Column column)
        {
            throw new AssertionError("Never call.");
        }

        @Override
        public void jsonColumn(Column column)
        {
            throw new AssertionError("Never call.");
        }

        @Override
        public void timestampColumn(Column column)
        {
            currentPageBuilder.setTimestamp(column, currentValue);
        }
    }
}
