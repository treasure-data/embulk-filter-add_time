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

package org.embulk.filter.add_time.reader;

import java.time.Instant;
import org.embulk.filter.add_time.converter.ValueConverter;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;

public class TimestampColumnReader
        extends AbstractColumnReader<TimestampColumnReader>
{
    protected Instant value;

    public TimestampColumnReader(ValueConverter valueConverter)
    {
        super(valueConverter);
    }

    @SuppressWarnings("deprecation")  // For use of PageReader#getTimestamp
    @Override
    public void readNonNullValue(Column column, PageReader pageReader)
    {
        value = pageReader.getTimestamp(column).getInstant();
    }

    @Override
    public void convertNonNullValue(Column column, PageBuilder pageBuilder)
    {
        valueConverter.convertValue(column, value, pageBuilder);
    }

    @Override
    public void copyValueTo(TimestampColumnReader columnReader)
    {
        columnReader.value = this.value;
    }
}
