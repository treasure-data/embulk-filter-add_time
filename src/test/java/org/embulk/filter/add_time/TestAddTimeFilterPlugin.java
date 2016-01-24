package org.embulk.filter.add_time;

import com.google.common.collect.ImmutableMap;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.filter.add_time.AddTimeFilterPlugin.PluginTask;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.spi.util.Pages;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

public class TestAddTimeFilterPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private AddTimeFilterPlugin plugin;
    private ConfigSource config;
    private Schema inputSchema;
    private List<Object[]> records;

    @Before
    public void createResources()
    {
        plugin = plugin();
        config = runtime.getExec().newConfigSource();
        inputSchema = schema("c0", Types.BOOLEAN, "c1", Types.LONG, "c2", Types.DOUBLE, "c3", Types.STRING, "c4", Types.TIMESTAMP);
    }

    @Test
    public void testFromColumn()
    {
        { // long type
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time"))
                    .set("from_column", ImmutableMap.of("name", "c1", "unix_timestamp_unit", "sec"));
            List<Page> pages = newPages(true, 1451646671L, 0.1, "foo", Timestamp.ofEpochSecond(1451646671));

            callTansaction(conf, inputSchema, pages);

            assertEquals(1, records.size());
            for (Object[] record : records) {
                assertEquals(inputSchema.size() + 1, record.length);

                assertEquals(true, record[0]);
                assertEquals(1451646671L, record[1]);
                assertEquals(0.1, record[2]);
                assertEquals("foo", record[3]);
                assertEquals(1451646671L, ((Timestamp) record[4]).getEpochSecond());
                assertEquals(1451646671L, ((Timestamp) record[5]).getEpochSecond());
            }
        }

        { // timestamp type
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time"))
                    .set("from_column", ImmutableMap.of("name", "c4"));
            List<Page> pages = newPages(true, 0L, 0.1, "foo", Timestamp.ofEpochSecond(1451646671));

            callTansaction(conf, inputSchema, pages);

            assertEquals(1, records.size());
            for (Object[] record : records) {
                assertEquals(inputSchema.size() + 1, record.length);

                assertEquals(true, record[0]);
                assertEquals(0L, record[1]);
                assertEquals(0.1, record[2]);
                assertEquals("foo", record[3]);
                assertEquals(1451646671L, ((Timestamp) record[4]).getEpochSecond());
                assertEquals(1451646671L, ((Timestamp) record[5]).getEpochSecond());
            }
        }

        { // string type
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time"))
                    .set("from_column", ImmutableMap.of("name", "c3"));
            List<Page> pages = newPages(true, 0L, 0.1, "2016-01-01 11:11:11 UTC", Timestamp.ofEpochSecond(1451646671));

            callTansaction(conf, inputSchema, pages);

            assertEquals(1, records.size());
            for (Object[] record : records) {
                assertEquals(inputSchema.size() + 1, record.length);

                assertEquals(true, record[0]);
                assertEquals(0L, record[1]);
                assertEquals(0.1, record[2]);
                assertEquals("2016-01-01 11:11:11 UTC", record[3]);
                assertEquals(1451646671L, ((Timestamp) record[4]).getEpochSecond());
                assertEquals(1451646671L, ((Timestamp) record[5]).getEpochSecond());
            }
        }
    }

    @Test
    public void testFromValue()
    {
        // mode: fixed_time
        {
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time"))
                    .set("from_value", ImmutableMap.of("mode", "fixed_time", "value", "2016-01-01 11:11:11 UTC"));
            List<Page> pages = newPages(true, 0L, 0.1, "foo", Timestamp.ofEpochSecond(1451646671));

            callTansaction(conf, inputSchema, pages);

            assertEquals(1, records.size());
            for (Object[] record : records) {
                assertEquals(inputSchema.size() + 1, record.length);

                assertEquals(true, record[0]);
                assertEquals(0L, record[1]);
                assertEquals(0.1, record[2]);
                assertEquals("foo", record[3]);
                assertEquals(1451646671L, ((Timestamp) record[4]).getEpochSecond());
                assertEquals(1451646671L, ((Timestamp) record[5]).getEpochSecond());
            }
        }
        { // specifies format
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time"))
                    .set("from_value", ImmutableMap.of("mode", "fixed_time", "value", "2016-01-01 11:11:11.000 UTC", "format", "%Y-%m-%d %H:%M:%S.%N %Z"));
            List<Page> pages = newPages(true, 0L, 0.1, "foo", Timestamp.ofEpochSecond(1451646671));

            callTansaction(conf, inputSchema, pages);

            assertEquals(1, records.size());
            for (Object[] record : records) {
                assertEquals(inputSchema.size() + 1, record.length);

                assertEquals(true, record[0]);
                assertEquals(0L, record[1]);
                assertEquals(0.1, record[2]);
                assertEquals("foo", record[3]);
                assertEquals(1451646671L, ((Timestamp) record[4]).getEpochSecond());
                assertEquals(1451646671L, ((Timestamp) record[5]).getEpochSecond());
            }
        }

        // mode: incremental_time
        {
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time"))
                    .set("from_value", ImmutableMap.of("mode", "incremental_time",
                            "from", "2016-01-01 11:11:11 UTC", "to", "2016-01-01 11:11:12 UTC"));
            List<Page> pages = newPages(true, 0L, 0.1, "foo", Timestamp.ofEpochSecond(1451646671));

            callTansaction(conf, inputSchema, pages);

            assertEquals(1, records.size());
            for (Object[] record : records) {
                assertEquals(inputSchema.size() + 1, record.length);

                assertEquals(true, record[0]);
                assertEquals(0L, record[1]);
                assertEquals(0.1, record[2]);
                assertEquals("foo", record[3]);
                assertEquals(1451646671L, ((Timestamp) record[4]).getEpochSecond());
                assertEquals(1451646671L, ((Timestamp) record[5]).getEpochSecond());
            }
        }
        { // specifies format
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time"))
                    .set("from_value", ImmutableMap.of("mode", "incremental_time",
                            "from", "2016-01-01 11:11:11.000 UTC", "to", "2016-01-01 11:11:12.000 UTC", "format", "%Y-%m-%d %H:%M:%S.%N %Z"));
            List<Page> pages = newPages(true, 0L, 0.1, "foo", Timestamp.ofEpochSecond(1451646671));

            callTansaction(conf, inputSchema, pages);

            assertEquals(1, records.size());
            for (Object[] record : records) {
                assertEquals(inputSchema.size() + 1, record.length);

                assertEquals(true, record[0]);
                assertEquals(0L, record[1]);
                assertEquals(0.1, record[2]);
                assertEquals("foo", record[3]);
                assertEquals(1451646671L, ((Timestamp) record[4]).getEpochSecond());
                assertEquals(1451646671L, ((Timestamp) record[5]).getEpochSecond());
            }
        }

        // mode: upload_time
        {
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time"))
                    .set("from_value", ImmutableMap.of("mode", "upload_time"));
            List<Page> pages = newPages(true, 0L, 0.1, "foo", Timestamp.ofEpochSecond(1451646671));

            callTansaction(conf, inputSchema, pages);

            assertEquals(1, records.size());
            for (Object[] record : records) {
                assertEquals(inputSchema.size() + 1, record.length);

                assertEquals(true, record[0]);
                assertEquals(0L, record[1]);
                assertEquals(0.1, record[2]);
                assertEquals("foo", record[3]);
                assertEquals(1451646671L, ((Timestamp) record[4]).getEpochSecond());
                assertEquals(runtime.getExec().getTransactionTime(), record[5]);
            }
        }
    }

    @Test
    public void testToColumn()
    {
        // timestamp type
        {
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time"))
                    .set("from_value", ImmutableMap.of("mode", "fixed_time", "value", "2016-01-01 11:11:11 UTC"));
            List<Page> pages = newPages(true, 0L, 0.1, "foo", Timestamp.ofEpochSecond(1451646671));

            callTansaction(conf, inputSchema, pages);

            assertEquals(1, records.size());
            for (Object[] record : records) {
                assertEquals(inputSchema.size() + 1, record.length);

                assertEquals(true, record[0]);
                assertEquals(0L, record[1]);
                assertEquals(0.1, record[2]);
                assertEquals("foo", record[3]);
                assertEquals(1451646671L, ((Timestamp) record[4]).getEpochSecond());
                assertEquals(1451646671L, ((Timestamp) record[5]).getEpochSecond());
            }
        }

        // long type
        { // unix_timestamp: sec
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time", "type", "long", "unix_timestamp_unit", "sec"))
                    .set("from_value", ImmutableMap.of("mode", "fixed_time", "value", "2016-01-01 11:11:11 UTC"));
            List<Page> pages = newPages(true, 0L, 0.1, "foo", Timestamp.ofEpochSecond(1451646671));

            callTansaction(conf, inputSchema, pages);

            assertEquals(1, records.size());
            for (Object[] record : records) {
                assertEquals(inputSchema.size() + 1, record.length);

                assertEquals(true, record[0]);
                assertEquals(0L, record[1]);
                assertEquals(0.1, record[2]);
                assertEquals("foo", record[3]);
                assertEquals(1451646671L, ((Timestamp) record[4]).getEpochSecond());
                assertEquals(1451646671L, record[5]);
            }
        }
        { // unix_timestamp: milli
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time", "type", "long", "unix_timestamp_unit", "milli"))
                    .set("from_value", ImmutableMap.of("mode", "fixed_time", "value", "2016-01-01 11:11:11 UTC"));
            List<Page> pages = newPages(true, 0L, 0.1, "foo", Timestamp.ofEpochSecond(1451646671));

            callTansaction(conf, inputSchema, pages);

            assertEquals(1, records.size());
            for (Object[] record : records) {
                assertEquals(inputSchema.size() + 1, record.length);

                assertEquals(true, record[0]);
                assertEquals(0L, record[1]);
                assertEquals(0.1, record[2]);
                assertEquals("foo", record[3]);
                assertEquals(1451646671L, ((Timestamp) record[4]).getEpochSecond());
                assertEquals(1451646671000L, record[5]);
            }
        }
        { // unix_timestamp: micro
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time", "type", "long", "unix_timestamp_unit", "micro"))
                    .set("from_value", ImmutableMap.of("mode", "fixed_time", "value", "2016-01-01 11:11:11 UTC"));
            List<Page> pages = newPages(true, 0L, 0.1, "foo", Timestamp.ofEpochSecond(1451646671));

            callTansaction(conf, inputSchema, pages);

            assertEquals(1, records.size());
            for (Object[] record : records) {
                assertEquals(inputSchema.size() + 1, record.length);

                assertEquals(true, record[0]);
                assertEquals(0L, record[1]);
                assertEquals(0.1, record[2]);
                assertEquals("foo", record[3]);
                assertEquals(1451646671L, ((Timestamp) record[4]).getEpochSecond());
                assertEquals(1451646671000000L, record[5]);
            }
        }
        { // unix_timestamp: nano
            ConfigSource conf = this.config.deepCopy()
                    .set("to_column", ImmutableMap.of("name", "time", "type", "long", "unix_timestamp_unit", "nano"))
                    .set("from_value", ImmutableMap.of("mode", "fixed_time", "value", "2016-01-01 11:11:11 UTC"));
            List<Page> pages = newPages(true, 0L, 0.1, "foo", Timestamp.ofEpochSecond(1451646671));

            callTansaction(conf, inputSchema, pages);

            assertEquals(1, records.size());
            for (Object[] record : records) {
                assertEquals(inputSchema.size() + 1, record.length);

                assertEquals(true, record[0]);
                assertEquals(0L, record[1]);
                assertEquals(0.1, record[2]);
                assertEquals("foo", record[3]);
                assertEquals(1451646671L, ((Timestamp) record[4]).getEpochSecond());
                assertEquals(1451646671000000000L, record[5]);
            }
        }
    }

    private List<Page> newPages(Object... values)
    {
        return PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema, values);
    }

    private void callTansaction(ConfigSource conf, final Schema inputSchema, final List<Page> pages)
    {
        final MockPageOutput output = new MockPageOutput();
        plugin.transaction(conf, inputSchema, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                try (PageOutput out = plugin.open(taskSource, inputSchema, outputSchema, output)) {
                    for (Page page : pages) {
                        out.add(page);
                    }
                    out.finish();
                }
                records = Pages.toObjects(outputSchema, output.pages);
            }
        });
    }

    public static Schema schema(Object... nameAndTypes)
    {
        Schema.Builder builder = Schema.builder();
        for (int i = 0; i < nameAndTypes.length; i += 2) {
            String name = (String) nameAndTypes[i];
            Type type = (Type) nameAndTypes[i + 1];
            builder.add(name, type);
        }
        return builder.build();
    }

    public static PluginTask pluginTask(ConfigSource config)
    {
        return config.loadConfig(PluginTask.class);
    }

    public static AddTimeFilterPlugin plugin()
    {
        return spy(new AddTimeFilterPlugin());
    }
}