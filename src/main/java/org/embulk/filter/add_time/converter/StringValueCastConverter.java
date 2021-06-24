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

import java.time.Instant;
import java.time.format.DateTimeParseException;
import org.embulk.filter.add_time.AddTimeFilterPlugin.FromColumnConfig;
import org.embulk.filter.add_time.AddTimeFilterPlugin.ToColumnConfig;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.util.timestamp.TimestampFormatter;

public class StringValueCastConverter
        extends ValueCastConverter
{
    private final TimestampFormatter fromTimestampFormatterForParsing;

    public StringValueCastConverter(FromColumnConfig fromColumnConfig, ToColumnConfig toColumnConfig)
    {
        super(toColumnConfig);
        final String pattern = fromColumnConfig.getFormat().orElse(fromColumnConfig.getDefaultTimestampFormat());
        this.fromTimestampFormatterForParsing = TimestampFormatter.builder(pattern, true)
                        .setDefaultZoneFromString(fromColumnConfig.getTimeZoneId().orElse(fromColumnConfig.getDefaultTimeZoneId()))
                        .setDefaultDateFromString(fromColumnConfig.getDate().orElse(fromColumnConfig.getDefaultDate()))
                        .build();
    }

    @Override
    public void convertValue(final Column column, String value, final PageBuilder pageBuilder)
    {
        columnVisitor.setValue(stringToInstant(value));
        columnVisitor.setPageBuilder(pageBuilder);
        column.visit(columnVisitor);
    }

    private Instant stringToInstant(final String value)
    {
        return this.fromTimestampFormatterForParsing.parse(value);
    }
}
