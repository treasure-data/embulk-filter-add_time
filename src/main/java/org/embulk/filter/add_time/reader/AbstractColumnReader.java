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

import org.embulk.filter.add_time.converter.ValueConverter;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;

public abstract class AbstractColumnReader<T extends AbstractColumnReader>
        implements ColumnReader<T>
{
    protected final ValueConverter valueConverter;
    protected boolean isNull = false;

    protected AbstractColumnReader(ValueConverter valueConverter)
    {
        this.valueConverter = valueConverter;
    }

    @Override
    public void readValue(Column column, PageReader pageReader)
    {
        if (!(isNull = pageReader.isNull(column))) {
            readNonNullValue(column, pageReader);
        }
    }

    protected abstract void readNonNullValue(Column column, PageReader pageReader);

    @Override
    public void convertValue(Column column, PageBuilder pageBuilder)
    {
        try {
            if (isNull) {
                valueConverter.convertNull(column, pageBuilder);
            }
            else {
                convertNonNullValue(column, pageBuilder);
            }
        }
        finally {
            isNull = false;
        }
    }

    protected abstract void convertNonNullValue(Column column, PageBuilder pageBuilder);

    @Override
    public void copyTo(T columnReader)
    {
        columnReader.isNull = this.isNull;
        copyValueTo(columnReader);
    }

    protected abstract void copyValueTo(T columnReader);
}
