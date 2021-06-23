package org.embulk.filter.add_time.converter;

import java.time.Instant;
import org.embulk.filter.add_time.AddTimeFilterPlugin.ToColumnConfig;
import org.embulk.filter.add_time.AddTimeFilterPlugin.UnixTimestampUnit;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ValueCastConverter
        implements ValueConverter
{
    protected final Logger log;
    protected final TimestampValueCastVisitor columnVisitor;

    public ValueCastConverter(ToColumnConfig toColumnConfig)
    {
        this.log = LoggerFactory.getLogger(ValueCastConverter.class);
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
    public void convertValue(Column column, final Instant value, final PageBuilder pageBuilder)
    {
        throw new AssertionError("Never call.");
    }

    static class TimestampValueCastVisitor
            implements ColumnVisitor
    {
        private final UnixTimestampUnit toUnixTimestampUnit;
        private PageBuilder currentPageBuilder;
        private Instant currentValue;

        TimestampValueCastVisitor(UnixTimestampUnit toUnixTimestampUnit)
        {
            this.toUnixTimestampUnit = toUnixTimestampUnit;
        }

        void setValue(final Instant value)
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

        @SuppressWarnings("deprecation")  // For use of org.embulk.spi.time.Timestamp
        @Override
        public void timestampColumn(Column column)
        {
            currentPageBuilder.setTimestamp(column, org.embulk.spi.time.Timestamp.ofInstant(currentValue));
        }
    }
}
