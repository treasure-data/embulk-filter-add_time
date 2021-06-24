/*
 * Copyright 2016 Treasure Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.filter.add_time;

import java.time.Instant;
import java.util.Optional;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.filter.add_time.converter.SchemaConverter;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public interface FromColumnConfig
            extends Task
    {
        @Config("name")
        String getName();

        @Config("unix_timestamp_unit")
        @ConfigDefault("\"sec\"")
        String getUnixTimestampUnit();

        // Duplicated with org.embulk.spi.time.TimestampParser.TimestampColumnOption below
        @Config("timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S %z\"") // override default value
        Optional<String> getFormat();

        // Duplicated with org.embulk.spi.time.TimestampParser.Task below
        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S %z\"") // override default value
        String getDefaultTimestampFormat();

        @Config("json_key")
        @ConfigDefault("null")
        Optional<String> getJsonKey();

        // From org.embulk.spi.time.TimestampParser.Task
        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        String getDefaultTimeZoneId();

        // "String getDefaultTimestampFormat()" existed in org.embulk.spi.time.TimestampParser.Task.
        //
        // But, it is duplicated with an existing method in FromColumnConfig with the same signature and the same @Config.
        // It is commented out, then.
        //
        // @Config("default_timestamp_format")
        // @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
        // String getDefaultTimestampFormat();

        // From org.embulk.spi.time.TimestampParser.Task
        @Config("default_date")
        @ConfigDefault("\"1970-01-01\"")
        String getDefaultDate();

        // From org.embulk.spi.time.TimestampParser.TimestampColumnOption
        @Config("timezone")
        @ConfigDefault("null")
        Optional<String> getTimeZoneId();

        // "Optional<String> getFormat()" existed in org.embulk.spi.time.TimestampParser.TimestampColumnOption.
        // But, it is duplicated with an existing method in FromColumnConfig with a different signature and a different @Config.
        //
        // A method declared directly in the interface has been prioritized.
        // It is commented out, then.
        //
        // @Config("format")
        // @ConfigDefault("null")
        // Optional<String> getFormat();

        // From org.embulk.spi.time.TimestampParser.TimestampColumnOption
        @Config("date")
        @ConfigDefault("null")
        Optional<String> getDate();
    }

    public interface FromValueConfig
            extends Task
    {
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

        // Duplicated with org.embulk.spi.time.TimestampParser.TimestampColumnOption below
        @Config("timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S %z\"") // override default value
        Optional<String> getFormat();

        // Duplicated with org.embulk.spi.time.TimestampParser.Task below
        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S %z\"") // override default value
        String getDefaultTimestampFormat();

        // From org.embulk.spi.time.TimestampParser.Task
        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        String getDefaultTimeZoneId();

        // "String getDefaultTimestampFormat()" existed in org.embulk.spi.time.TimestampParser.Task.
        //
        // But, it is duplicated with an existing method in FromColumnConfig with the same signature and the same @Config.
        // It is commented out, then.
        //
        // @Config("default_timestamp_format")
        // @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
        // String getDefaultTimestampFormat();

        // From org.embulk.spi.time.TimestampParser.Task
        @Config("default_date")
        @ConfigDefault("\"1970-01-01\"")
        String getDefaultDate();

        // From org.embulk.spi.time.TimestampParser.TimestampColumnOption
        @Config("timezone")
        @ConfigDefault("null")
        Optional<String> getTimeZoneId();

        // "Optional<String> getFormat()" existed in org.embulk.spi.time.TimestampParser.TimestampColumnOption.
        // But, it is duplicated with an existing method in FromColumnConfig with a different signature and a different @Config.
        //
        // A method declared directly in the interface has been prioritized.
        // It is commented out, then.
        //
        // @Config("format")
        // @ConfigDefault("null")
        // Optional<String> getFormat();

        // From org.embulk.spi.time.TimestampParser.TimestampColumnOption
        @Config("date")
        @ConfigDefault("null")
        Optional<String> getDate();
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

        public long toLong(final Instant t)
        {
            return t.getEpochSecond() * secondUnit + t.getNano() / nanoUnit;
        }

        public Instant toInstant(final long t)
        {
            return Instant.ofEpochSecond(t / secondUnit, (int) (t % secondUnit * nanoUnit));
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

    @SuppressWarnings("deprecation")  // For use of Task#dump()
    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        control.run(task.dump(), new SchemaConverter(log, task, inputSchema).toOutputSchema());
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema,
            Schema outputSchema, PageOutput output)
    {
        final PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);
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
            this.pageReader = Exec.getPageReader(inputSchema);
            this.pageBuilder = Exec.getPageBuilder(Exec.getBufferAllocator(), outputSchema, output);
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

    private static final Logger log = LoggerFactory.getLogger(AddTimeFilterPlugin.class);

    static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();
    static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();
}
