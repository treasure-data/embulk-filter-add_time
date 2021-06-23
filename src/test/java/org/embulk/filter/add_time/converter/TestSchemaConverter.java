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

package org.embulk.filter.add_time.converter;

import com.google.common.collect.ImmutableMap;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import static org.embulk.filter.add_time.TestAddTimeFilterPlugin.pluginTask;
import static org.embulk.filter.add_time.TestAddTimeFilterPlugin.schema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestSchemaConverter
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private static final Logger log = LoggerFactory.getLogger(TestSchemaConverter.class);
    private ConfigSource config;

    @Before
    public void createResources()
    {
        config = runtime.getExec().newConfigSource();
    }

    @Test
    public void validate()
    {
        // if to_column doesn't exist, throws ConfigException
        {
            ConfigSource conf = this.config.deepCopy()
                    .set("from_value", ImmutableMap.of("mode", "upload_time"));
            failSchemaConverterCreation(log, conf, schema("c0", Types.TIMESTAMP));
        }

        // if both from_column and from_value exist, throws ConfigException
        {
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time"))
                    .set("from_column", ImmutableMap.of("name", "c0"))
                    .set("from_value", ImmutableMap.of("mode", "upload_time"));
            failSchemaConverterCreation(log, conf, schema("c0", Types.TIMESTAMP));
        }

        // if from_column or from_value doesn't exist, throws ConfigException
        {
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time"));
            failSchemaConverterCreation(log, conf, schema("c0", Types.TIMESTAMP));
        }

        // the column specified as from_column doesn't exist in the schema, throws ConfigException
        {
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time"))
                    .set("from_column", ImmutableMap.of("name", "c1"));;
            failSchemaConverterCreation(log, conf, schema("c0", Types.TIMESTAMP));
        }

        // if from_value type is not string or long, throws ConfigException
        {
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time"))
                    .set("from_value", ImmutableMap.of("mode", "fixed_time", "value", new ArrayList<Long>()));
            failSchemaConverterCreation(log, conf, schema("c0", Types.TIMESTAMP));
        }
        {
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time"))
                    .set("from_value", ImmutableMap.of("mode", "incremental_time", "from", new ArrayList<Long>(), "to", new ArrayList<Long>()));
            failSchemaConverterCreation(log, conf, schema("c0", Types.TIMESTAMP));
        }

        // the type of the column specified as to_column must be 'long' or 'timestamp'
        { // boolean
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time", "type", "boolean"))
                    .set("from_column", ImmutableMap.of("name", "c0"));
            failSchemaConverterCreation(log, conf, schema("c0", Types.TIMESTAMP));
        }
        { // double
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time", "type", "double"))
                    .set("from_column", ImmutableMap.of("name", "c0"));
            failSchemaConverterCreation(log, conf, schema("c0", Types.TIMESTAMP));
        }
        { // string
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time", "type", "string"))
                    .set("from_column", ImmutableMap.of("name", "c0"));
            failSchemaConverterCreation(log, conf, schema("c0", Types.TIMESTAMP));
        }

        // the type of the column specified as from_column must be 'long', 'timestamp' or 'string'
        {
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time"))
                    .set("from_column", ImmutableMap.of("name", "c0"));
            failSchemaConverterCreation(log, conf, schema("c0", Types.BOOLEAN)); // boolean
            failSchemaConverterCreation(log, conf, schema("c0", Types.DOUBLE)); // double
        }
    }

    private static void failSchemaConverterCreation(Logger log, ConfigSource conf, Schema inputSchema)
    {
        try {
            new SchemaConverter(log, pluginTask(conf), inputSchema);
            fail();
        }
        catch (Throwable t) {
            assertTrue(t instanceof ConfigException);
        }
    }

    @Test
    public void testToColumn()
    {
        ConfigSource config = this.config.deepCopy().set("from_value", ImmutableMap.of("mode", "upload_time"));

        // timestamp type
        { // default type is timestamp
            ConfigSource conf = config.deepCopy().set("to_column", ImmutableMap.of("name", "time"));
            Schema inputSchema = schema("c0", Types.TIMESTAMP);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(inputSchema.getColumn(0), outputSchema.getColumn(0));
            assertEquals(new Column(1, "time", Types.TIMESTAMP), outputSchema.getColumn(1));
        }
        {
            ConfigSource conf = config.deepCopy().set("to_column", ImmutableMap.of("name", "time", "type", "timestamp"));
            Schema inputSchema = schema("c0", Types.TIMESTAMP);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(inputSchema.getColumn(0), outputSchema.getColumn(0));
            assertEquals(new Column(1, "time", Types.TIMESTAMP), outputSchema.getColumn(1));
        }

        // long type
        { // default unix_timestamp_unit is sec
            ConfigSource conf = config.deepCopy().set("to_column", ImmutableMap.of("name", "time", "type", "long"));
            Schema inputSchema = schema("c0", Types.TIMESTAMP);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(inputSchema.getColumn(0), outputSchema.getColumn(0));
            assertEquals(new Column(1, "time", Types.LONG), outputSchema.getColumn(1));
        }
        { // default unix_timestamp_unit is sec
            ConfigSource conf = config.deepCopy().set("to_column", ImmutableMap.of("name", "time", "type", "long", "unix_timestamp_unit", "milli"));
            Schema inputSchema = schema("c0", Types.TIMESTAMP);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(inputSchema.getColumn(0), outputSchema.getColumn(0));
            assertEquals(new Column(1, "time", Types.LONG), outputSchema.getColumn(1));
        }

        // renaming
        { // timestamp type
            ConfigSource conf = config.deepCopy().set("to_column", ImmutableMap.of("name", "c0"));
            Schema inputSchema = schema("c0", Types.TIMESTAMP);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(new Column(0, "c0_", Types.TIMESTAMP), outputSchema.getColumn(0));
            assertEquals(new Column(1, "c0", Types.TIMESTAMP), outputSchema.getColumn(1));
        }
        { // timestamp type
            ConfigSource conf = config.deepCopy().set("to_column", ImmutableMap.of("name", "c0"));
            Schema inputSchema = schema("c0", Types.LONG);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(new Column(0, "c0_", Types.LONG), outputSchema.getColumn(0));
            assertEquals(new Column(1, "c0", Types.TIMESTAMP), outputSchema.getColumn(1));
        }
    }

    @Test
    public void testFromColumn()
    {
        ConfigSource config = this.config.deepCopy().set("to_column", ImmutableMap.of("name", "time"));

        // timestamp type
        {
            ConfigSource conf = config.deepCopy().set("from_column", ImmutableMap.of("name", "c0"));
            Schema inputSchema = schema("c0", Types.TIMESTAMP);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(inputSchema.getColumn(0), outputSchema.getColumn(0));
            assertEquals(new Column(1, "time", Types.TIMESTAMP), outputSchema.getColumn(1));
        }

        // long type
        {
            ConfigSource conf = config.deepCopy().set("from_column", ImmutableMap.of("name", "c0"));
            Schema inputSchema = schema("c0", Types.LONG);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(inputSchema.getColumn(0), outputSchema.getColumn(0));
            assertEquals(new Column(1, "time", Types.TIMESTAMP), outputSchema.getColumn(1));
        }
        {
            ConfigSource conf = config.deepCopy().set("from_column", ImmutableMap.of("name", "c0", "unix_timestamp_unit", "sec"));
            Schema inputSchema = schema("c0", Types.LONG);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(inputSchema.getColumn(0), outputSchema.getColumn(0));
            assertEquals(new Column(1, "time", Types.TIMESTAMP), outputSchema.getColumn(1));
        }

        // string type
        {
            ConfigSource conf = config.deepCopy().set("from_column", ImmutableMap.of("name", "c0"));
            Schema inputSchema = schema("c0", Types.STRING);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(inputSchema.getColumn(0), outputSchema.getColumn(0));
            assertEquals(new Column(1, "time", Types.TIMESTAMP), outputSchema.getColumn(1));
        }
        {
            ConfigSource conf = config.deepCopy().set("from_column", ImmutableMap.of("name", "c0", "timestamp_format", "%Y-%m-%d %H:%M:%S.%N %Z"));
            Schema inputSchema = schema("c0", Types.STRING);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(inputSchema.getColumn(0), outputSchema.getColumn(0));
            assertEquals(new Column(1, "time", Types.TIMESTAMP), outputSchema.getColumn(1));
        }
    }

    @Test
    public void testFromValue()
    {
        ConfigSource config = this.config.deepCopy().set("to_column", ImmutableMap.of("name", "time"));

        // mode: fixed_time
        { // default mode
            ConfigSource conf = config.deepCopy().set("from_value", ImmutableMap.of("value", "2016-01-01 10:10:10 UTC"));
            Schema inputSchema = schema("c0", Types.STRING);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(inputSchema.getColumn(0), outputSchema.getColumn(0));
            assertEquals(new Column(1, "time", Types.TIMESTAMP), outputSchema.getColumn(1));
        }
        {
            ConfigSource conf = config.deepCopy().set("from_value", ImmutableMap.of("mode", "fixed_time", "value", "2016-01-01 10:10:10 UTC"));
            Schema inputSchema = schema("c0", Types.STRING);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(inputSchema.getColumn(0), outputSchema.getColumn(0));
            assertEquals(new Column(1, "time", Types.TIMESTAMP), outputSchema.getColumn(1));
        }
        { // specify 'timestamp_format'
            ConfigSource conf = config.deepCopy()
                    .set("from_value", ImmutableMap.of("mode", "fixed_time", "value", "2016-01-01 10:10:10.000 UTC",
                            "timestamp_format", "%Y-%m-%d %H:%M:%S.%N %Z"));
            Schema inputSchema = schema("c0", Types.STRING);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(inputSchema.getColumn(0), outputSchema.getColumn(0));
            assertEquals(new Column(1, "time", Types.TIMESTAMP), outputSchema.getColumn(1));
        }
        { // specify 'unix_timestamp_unit'
            ConfigSource conf = config.deepCopy()
                    .set("from_value", ImmutableMap.of("mode", "fixed_time", "value", 1451643010,
                            "unix_timestamp_unit", "sec"));
            Schema inputSchema = schema("c0", Types.STRING);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(inputSchema.getColumn(0), outputSchema.getColumn(0));
            assertEquals(new Column(1, "time", Types.TIMESTAMP), outputSchema.getColumn(1));
        }

        // mode: incremental_time
        {
            ConfigSource conf = config.deepCopy()
                    .set("from_value", ImmutableMap.of("mode", "incremental_time",
                            "from", "2016-01-01 10:10:10 UTC", "to", "2016-01-01 10:10:10 UTC"));
            Schema inputSchema = schema("c0", Types.STRING);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(inputSchema.getColumn(0), outputSchema.getColumn(0));
            assertEquals(new Column(1, "time", Types.TIMESTAMP), outputSchema.getColumn(1));
        }
        { // specify 'timestamp_format'
            ConfigSource conf = config.deepCopy()
                    .set("from_value", ImmutableMap.of("mode", "incremental_time",
                            "from", "2016-01-01 10:10:10.000 UTC", "to", "2016-01-01 10:10:10.000 UTC", "timestamp_format", "%Y-%m-%d %H:%M:%S.%N %Z"));
            Schema inputSchema = schema("c0", Types.STRING);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(inputSchema.getColumn(0), outputSchema.getColumn(0));
            assertEquals(new Column(1, "time", Types.TIMESTAMP), outputSchema.getColumn(1));
        }
        { // specify 'unix_timestamp_unit'
            ConfigSource conf = config.deepCopy()
                    .set("from_value", ImmutableMap.of("mode", "incremental_time",
                            "from", 1451643010, "to", 1451643010, "unix_timestamp_unit", "sec"));
            Schema inputSchema = schema("c0", Types.STRING);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(inputSchema.getColumn(0), outputSchema.getColumn(0));
            assertEquals(new Column(1, "time", Types.TIMESTAMP), outputSchema.getColumn(1));
        }

        // mode: upload_time
        {
            ConfigSource conf = config.deepCopy()
                    .set("from_value", ImmutableMap.of(
                            "mode", "upload_time"));
            Schema inputSchema = schema("c0", Types.STRING);
            Schema outputSchema = new SchemaConverter(log, pluginTask(conf), inputSchema).toOutputSchema();

            assertEquals(inputSchema.size() + 1, outputSchema.size());
            assertEquals(inputSchema.getColumn(0), outputSchema.getColumn(0));
            assertEquals(new Column(1, "time", Types.TIMESTAMP), outputSchema.getColumn(1));
        }
    }

}
