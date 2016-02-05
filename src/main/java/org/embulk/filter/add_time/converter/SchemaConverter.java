package org.embulk.filter.add_time.converter;

import com.google.common.base.Optional;
import org.embulk.config.ConfigException;
import org.embulk.filter.add_time.AddTimeFilterPlugin.PluginTask;
import org.embulk.filter.add_time.AddTimeFilterPlugin.FromColumnConfig;
import org.embulk.filter.add_time.AddTimeFilterPlugin.FromValueConfig;
import org.embulk.filter.add_time.AddTimeFilterPlugin.ToColumnConfig;
import org.embulk.filter.add_time.reader.BooleanColumnReader;
import org.embulk.filter.add_time.reader.ColumnReader;
import org.embulk.filter.add_time.reader.DoubleColumnReader;
import org.embulk.filter.add_time.reader.LongColumnReader;
import org.embulk.filter.add_time.reader.StringColumnReader;
import org.embulk.filter.add_time.reader.TimeValueGenerator;
import org.embulk.filter.add_time.reader.TimestampColumnReader;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.DataException;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.slf4j.Logger;

public class SchemaConverter
{
    private final Logger log;
    private final ColumnConverter[] converters;

    public SchemaConverter(Logger log, PluginTask task, Schema inputSchema)
    {
        this.log = log;

        ToColumnConfig toColumnConfig = task.getToColumn();
        final String toColumnName = toColumnConfig.getName();
        final Type toColumnType = toToColumnType(toColumnName, toColumnConfig.getType()); // TODO getType should return Type object

        Optional<FromColumnConfig> fromColumnConfig = task.getFromColumn();
        Optional<FromValueConfig> fromValueConfig = task.getFromValue();

        if (fromColumnConfig.isPresent() && fromValueConfig.isPresent()) {
            throw new ConfigException("Setting both from_column and from_value is invalid.");
        }
        if (!fromColumnConfig.isPresent() && !fromValueConfig.isPresent()) {
            throw new ConfigException("Setting from_column or from_value is required.");
        }
        if (fromColumnConfig.isPresent() && !hasColumn(fromColumnConfig.get().getName(), inputSchema)) {
            throw new ConfigException(String.format("from_column '%s' doesn't exist in the schema.", fromColumnConfig.get().getName()));
        }

        converters = new ColumnConverter[inputSchema.size() + 1];

        for (int i = 0; i < inputSchema.size(); i++) {
            Column column = inputSchema.getColumn(i);
            String columnName = column.getName();
            Type columnType = column.getType();

            final String newColumnName;
            if (columnName.equals(toColumnName)) {
                newColumnName = newColumnUniqueName(columnName, inputSchema);
                log.warn("to_column '{}' is set but '{}' column also exists. The existent '{}' column is renamed to '{}'.",
                        toColumnName, toColumnName, toColumnName, newColumnName);
            }
            else {
                newColumnName = columnName;
            }

            if (fromColumnConfig.isPresent() && columnName.equals(fromColumnConfig.get().getName())) {
                if (!columnType.equals(Types.LONG) && !columnType.equals(Types.STRING) && !columnType.equals(Types.TIMESTAMP)) {
                    throw new ConfigException(String.format(
                            "The type of the '%s' column specified as from_column must be long, string or timestamp. But it's %s.", columnName, columnType));
                }

                ColumnReader duplicatee = newColumnReader(columnType, newValueCastConverter(columnType, fromColumnConfig, toColumnConfig));
                converters[inputSchema.size()] = new SimpleColumnConverter.Builder()
                        .setColumn(new Column(inputSchema.size(), toColumnName, toColumnType))
                        .setColumnReader(duplicatee)
                        .build();
                converters[i] = new ColumnDuplicator.Builder()
                        .setColumn(new Column(i, newColumnName, columnType))
                        .setDuplicator(newColumnReader(columnType, ValueConverter.NO_CONV))
                        .setDuplicatee(duplicatee)
                        .build();
            }
            else {
                converters[i] = new SimpleColumnConverter.Builder()
                        .setColumn(new Column(i, newColumnName, columnType))
                        .setColumnReader(newColumnReader(columnType, ValueConverter.NO_CONV))
                        .build();
            }
        }

        if (fromValueConfig.isPresent()) {
            // create column converter for from_value
            converters[inputSchema.size()] = new SimpleColumnConverter.Builder()
                    .setColumn(new Column(inputSchema.size(), toColumnName, toColumnType))
                    .setColumnReader(TimeValueGenerator.newGenerator(fromValueConfig.get(), newValueCastConverter(Types.TIMESTAMP, fromColumnConfig, toColumnConfig)))
                    .build();
        }
    }

    private static boolean hasColumn(String columnName, Schema schema)
    {
        for (Column c : schema.getColumns()) {
            if (c.getName().equals(columnName)) {
                return true;
            }
        }
        return false;
    }

    private static String newColumnUniqueName(String originalName, Schema schema)
    {
        String name = originalName;
        do {
            name += "_";
        }
        while (containsColumnName(schema, name));
        return name;
    }

    private static boolean containsColumnName(Schema schema, String name)
    {
        for (Column c : schema.getColumns()) {
            if (c.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static ColumnReader newColumnReader(Type columnType, ValueConverter valueConverter)
    {
        if (columnType instanceof BooleanType) {
            return new BooleanColumnReader(valueConverter);
        }
        else if (columnType instanceof LongType) {
            return new LongColumnReader(valueConverter);
        }
        else if (columnType instanceof DoubleType) {
            return new DoubleColumnReader(valueConverter);
        }
        else if (columnType instanceof StringType) {
            return new StringColumnReader(valueConverter);
        }
        else if (columnType instanceof TimestampType) {
            return new TimestampColumnReader(valueConverter);
        }
        // TODO support Json type
        else {
            throw new ConfigException("Unsupported type: " + columnType); // TODO after json type support, it should be changed to AssertionError.
        }
    }

    private static ValueCastConverter newValueCastConverter(Type columnType, Optional<FromColumnConfig> fromColumnConfig, ToColumnConfig toColumnConfig)
    {
        if (columnType instanceof LongType) {
            return new LongValueCastConverter(fromColumnConfig.get(), toColumnConfig);
        }
        else if (columnType instanceof StringType) {
            return new StringValueCastConverter(fromColumnConfig.get(), toColumnConfig);
        }
        else if (columnType instanceof TimestampType) {
            return new TimestampValueCastConverter(toColumnConfig);
        }
        else {
            throw new AssertionError("Unsupported type: " + columnType);
        }
    }

    private static Type toToColumnType(String name, String type)
    {
        switch (type) {
            case "long":
                return Types.LONG;
            case "timestamp":
                return Types.TIMESTAMP;
            default:
                throw new ConfigException(String.format( // TODO should return AssertionError
                        "The type of the '{}' column specified as to_column must be long or timestamp. But it's {}.", name, type));
        }
    }

    public void convertRecord(final PageReader pageReader, final PageBuilder pageBuilder)
    {
        try {
            beginRecordConversion();

            pageReader.getSchema().visitColumns(new ColumnVisitor()
            {
                @Override
                public void booleanColumn(Column column)
                {
                    updateColumn(column, pageReader);
                }

                @Override
                public void longColumn(Column column)
                {
                    updateColumn(column, pageReader);
                }

                @Override
                public void doubleColumn(Column column)
                {
                    updateColumn(column, pageReader);
                }

                @Override
                public void stringColumn(Column column)
                {
                    updateColumn(column, pageReader);
                }

                @Override
                public void timestampColumn(Column column)
                {
                    updateColumn(column, pageReader);
                }
            });

            endRecordConversion(pageBuilder);
        }
        catch (RuntimeException e) { // TODO should use AddTimeRecordValidateException or the subclasses
            log.warn(String.format("Skipped a record (%s).", e.getMessage()), e);
        }
    }

    private void beginRecordConversion()
    {
    }

    private void updateColumn(Column column, PageReader pageReader)
    {
        converters[column.getIndex()].update(pageReader);
    }

    private void endRecordConversion(PageBuilder pageBuilder)
    {
        for (ColumnConverter converter : converters) {
            converter.convert(pageBuilder);
        }

        pageBuilder.addRecord();
    }

    public Schema toOutputSchema()
    {
        Schema.Builder schemaBuilder = new Schema.Builder();
        for (ColumnConverter converter : converters) {
            converter.addColumn(schemaBuilder);
        }
        return schemaBuilder.build();
    }

    static class AddTimeRecordValidateException
            extends DataException
    {
        AddTimeRecordValidateException(Throwable cause)
        {
            super(cause);
        }
    }
}
