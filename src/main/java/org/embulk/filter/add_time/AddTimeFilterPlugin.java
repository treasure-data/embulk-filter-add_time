package org.embulk.filter.add_time;

import java.util.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.filter.add_time.converter.SchemaConverter;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.slf4j.Logger;

public class AddTimeFilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("to_column")
        ToColumnConfig getToColumn();

        @Config("from_column")
        @ConfigDefault("null")
        Optional<FromColumnConfig> getFromColumn();

        @Config("from_value")
        @ConfigDefault("null")
        Optional<FromValueConfig> getFromValue();
    }

    public interface ToColumnConfig
            extends Task
    {
        @Config("name")
        String getName();

        @Config("type")
        @ConfigDefault("\"timestamp\"")
        String getType();

        @Config("unix_timestamp_unit")
        @ConfigDefault("\"sec\"")
        String getUnixTimestampUnit();
    }

    public interface FromColumnConfig extends Task
    {
        // Came from org.embulk.spi.time.TimestampParser.Task
        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        String getDefaultTimeZoneId();

        // Came from org.embulk.spi.time.TimestampParser.Task
        // But, this is overridden below.
        // @Config("default_timestamp_format")
        // @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
        // String getDefaultTimestampFormat();

        // Came from org.embulk.spi.time.TimestampParser.Task
        @Config("default_date")
        @ConfigDefault("\"1970-01-01\"")
        String getDefaultDate();

        // Came from org.embulk.spi.time.TimestampParser.TimestampColumnOption
        @Config("timezone")
        @ConfigDefault("null")
        Optional<String> getTimeZoneId();

        // Came from org.embulk.spi.time.TimestampParser.TimestampColumnOption
        // But, this is overridden below.
        // @Config("format")
        // @ConfigDefault("null")
        // Optional<String> getFormat();

        // Came from org.embulk.spi.time.TimestampParser.TimestampColumnOption
        @Config("date")
        @ConfigDefault("null")
        Optional<String> getDate();

        @Config("name")
        String getName();

        @Config("unix_timestamp_unit")
        @ConfigDefault("\"sec\"")
        String getUnixTimestampUnit();

        @Config("timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S %z\"") // override default value
        Optional<String> getFormat();

        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S %z\"") // override default value
        String getDefaultTimestampFormat();

        @Config("json_key")
        @ConfigDefault("null")
        Optional<String> getJsonKey();
    }

    public interface FromValueConfig extends Task
    {
        // Came from org.embulk.spi.time.TimestampParser.Task
        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        String getDefaultTimeZoneId();

        // Came from org.embulk.spi.time.TimestampParser.Task
        // But, this is overridden below.
        // @Config("default_timestamp_format")
        // @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
        // String getDefaultTimestampFormat();

        // Came from org.embulk.spi.time.TimestampParser.Task
        @Config("default_date")
        @ConfigDefault("\"1970-01-01\"")
        String getDefaultDate();

        // Came from org.embulk.spi.time.TimestampParser.TimestampColumnOption
        @Config("timezone")
        @ConfigDefault("null")
        Optional<String> getTimeZoneId();

        // Came from org.embulk.spi.time.TimestampParser.TimestampColumnOption
        // But, this is overridden below.
        // @Config("format")
        // @ConfigDefault("null")
        // Optional<String> getFormat();

        // Came from org.embulk.spi.time.TimestampParser.TimestampColumnOption
        @Config("date")
        @ConfigDefault("null")
        Optional<String> getDate();

        @Config("mode")
        @ConfigDefault("\"fixed_time\"")
        String getMode();

        @Config("value")
        @ConfigDefault("null")
        Optional<Object> getValue();

        @Config("from")
        @ConfigDefault("null")
        Optional<Object> getFrom();

        @Config("to")
        @ConfigDefault("null")
        Optional<Object> getTo();

        @Config("unix_timestamp_unit")
        @ConfigDefault("\"sec\"")
        String getUnixTimestampUnit();

        @Config("timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S %z\"") // override default value
        Optional<String> getFormat();

        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S %z\"") // override default value
        String getDefaultTimestampFormat();
    }

    public enum UnixTimestampUnit
    {
        SEC(1, 1000000000),
        MILLI(1000, 1000000),
        MICRO(1000000, 1000),
        NANO(1000000000, 1);

        private final int secondUnit;
        private final int nanoUnit;

        UnixTimestampUnit(int secondUnit, int nanoUnit)
        {
            this.secondUnit = secondUnit;
            this.nanoUnit = nanoUnit;
        }

        public long toLong(Timestamp t)
        {
            return t.getEpochSecond() * secondUnit + t.getNano() / nanoUnit;
        }

        public Timestamp toTimestamp(long t)
        {
            return Timestamp.ofEpochSecond(t / secondUnit, (int) (t % secondUnit * nanoUnit));
        }

        public static UnixTimestampUnit of(String s)
        {
            switch (s) {
            case "sec": return SEC;
            case "milli": return MILLI;
            case "micro": return MICRO;
            case "nano": return NANO;
            default:
                throw new ConfigException(
                        String.format("Unknown unix_timestamp_unit '%s'. Supported units are sec, milli, micro, and nano", s));
            }
        }
    }

    private final Logger log;

    public AddTimeFilterPlugin()
    {
        this.log = Exec.getLogger(getClass());
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        control.run(task.dump(), new SchemaConverter(log, task, inputSchema).toOutputSchema());
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema,
            Schema outputSchema, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        return new PageConverter(log, inputSchema, outputSchema, output, new SchemaConverter(log, task, inputSchema));
    }

    static class PageConverter
            implements PageOutput
    {
        private final Logger log;
        private SchemaConverter schemaConverter;
        private final PageReader pageReader;
        private final PageBuilder pageBuilder;

        public PageConverter(Logger log, Schema inputSchema, Schema outputSchema, PageOutput output, SchemaConverter schemaConverter)
        {
            this.log = log;
            this.schemaConverter = schemaConverter;
            this.pageReader = new PageReader(inputSchema);
            this.pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
        }

        @Override
        public void add(Page page)
        {
            pageReader.setPage(page);

            while (pageReader.nextRecord()) {
                schemaConverter.convertRecord(pageReader, pageBuilder);
            }
        }

        @Override
        public void finish()
        {
            pageBuilder.finish();
        }

        @Override
        public void close()
        {
            pageBuilder.close();
        }
    }

}
