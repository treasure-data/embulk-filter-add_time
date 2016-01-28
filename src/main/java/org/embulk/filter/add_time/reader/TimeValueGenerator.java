package org.embulk.filter.add_time.reader;

import com.google.common.base.Optional;
import org.embulk.config.ConfigException;
import org.embulk.filter.add_time.AddTimeFilterPlugin.FromValueConfig;
import org.embulk.filter.add_time.AddTimeFilterPlugin.UnixTimestampUnit;
import org.embulk.filter.add_time.converter.ValueConverter;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;

public abstract class TimeValueGenerator
        implements ColumnReader
{
    private final ValueConverter valueConverter;

    public TimeValueGenerator(ValueConverter valueConverter)
    {
        this.valueConverter = valueConverter;
    }

    protected abstract Timestamp nextTimestamp();

    @Override
    public void readValue(Column column, PageReader pageReader)
    {
        throw new AssertionError("Never call");
    }

    @Override
    public void convertValue(Column column, PageBuilder pageBuilder)
    {
        valueConverter.convertValue(column, nextTimestamp(), pageBuilder);
    }

    @Override
    public void copyTo(ColumnReader columnReader)
    {
        throw new AssertionError("Never call");
    }

    public static TimeValueGenerator newGenerator(final FromValueConfig config, ValueConverter valueConverter)
    {
        switch (config.getMode()) {
            case "fixed_time":
                require(config.getValue(), "'value'");
                reject(config.getFrom(), "'from'");
                reject(config.getTo(), "'to'");
                return new FixedTimeValueGenerator(config, valueConverter);

            case "incremental_time": // default mode
                require(config.getFrom(), "'from', 'to'");
                require(config.getTo(), "'to'");
                reject(config.getValue(), "'value'");
                return new IncrementalTimeValueGenerator(config, valueConverter);

            case "upload_time":
                reject(config.getFrom(), "'value'");
                reject(config.getFrom(), "'from'");
                reject(config.getTo(), "'to'");
                return new UploadTimeValueGenerator(valueConverter);

            default:
                throw new ConfigException(String.format("Unknwon mode '%s'. Supported methods are incremental_time, fixed_time.", config.getMode()));
        }
    }

    public static class IncrementalTimeValueGenerator
            extends TimeValueGenerator
    {
        private final Timestamp from;
        private final Timestamp to;

        private Timestamp current;

        public IncrementalTimeValueGenerator(final FromValueConfig config, ValueConverter valueConverter)
        {
            super(valueConverter);
            current = from = toTimestamp(config, config.getFrom().get());
            to = toTimestamp(config, config.getTo().get());
        }

        @Override
        public Timestamp nextTimestamp()
        {
            try {
                Timestamp ret = current;
                current = Timestamp.ofEpochSecond(current.getEpochSecond() + 1, current.getNano());
                return ret;
            }
            finally {
                if (current.compareTo(to) > 0) {
                    current = from;
                }
            }
        }
    }

    public static class FixedTimeValueGenerator
            extends TimeValueGenerator
    {
        private final Timestamp value;

        public FixedTimeValueGenerator(FromValueConfig config, ValueConverter valueConverter)
        {
            this(toTimestamp(config, config.getValue().get()), valueConverter);
        }

        public FixedTimeValueGenerator(Timestamp value, ValueConverter valueConverter)
        {
            super(valueConverter);
            this.value = value;
        }

        @Override
        public Timestamp nextTimestamp()
        {
            return value;
        }

    }

    public static class UploadTimeValueGenerator
            extends FixedTimeValueGenerator
    {
        public UploadTimeValueGenerator(ValueConverter valueConverter)
        {
            super(Exec.getTransactionTime(), valueConverter);
        }
    }

    // ported from embulk-input-s3
    private static <T> T require(Optional<T> value, String message)
    {
        if (value.isPresent()) {
            return typeCheck(value.get(), message);
        }
        else {
            throw new ConfigException("Required option is not set: " + message);
        }
    }

    // ported from embulk-input-s3
    private static <T> void reject(Optional<T> value, String message)
    {
        if (value.isPresent()) {
            throw new ConfigException("Invalid option is set: " + message);
        }
    }

    private static <T> T typeCheck(T value, String message)
    {
        if (value instanceof String || value instanceof Number) {
            return value;
        }
        else {
            throw new ConfigException("Required option must be string or long: " + message);
        }
    }

    private static Timestamp toTimestamp(FromValueConfig config, Object time)
    {
        if (time instanceof String) {
            return new TimestampParser(config, config).parse((String) time); // TODO optimize?
        }
        else if (time instanceof Number) {
            long t = ((Number) time).longValue();
            return UnixTimestampUnit.of(config.getUnixTimestampUnit()).toTimestamp(t);
        }
        else {
            throw new RuntimeException();
        }
    }
}
