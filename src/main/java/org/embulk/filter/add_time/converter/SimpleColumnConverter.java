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

import org.embulk.filter.add_time.reader.ColumnReader;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;

public class SimpleColumnConverter
        implements ColumnConverter
{
    public static class Builder
    {
        private Column column;
        private ColumnReader reader;

        public Builder()
        {
        }

        public Builder setColumn(Column column)
        {
            this.column = column;
            return this;
        }

        public Builder setColumnReader(ColumnReader reader)
        {
            this.reader = reader;
            return this;
        }

        public ColumnConverter build()
        {
            return new SimpleColumnConverter(column, reader);
        }
    }

    private final Column column;
    private final ColumnReader columnReader;

    private SimpleColumnConverter(Column column, ColumnReader columnReader)
    {
        this.column = column;
        this.columnReader = columnReader;
    }

    public void update(PageReader pageReader)
    {
        columnReader.readValue(column, pageReader);
    }

    public void convert(PageBuilder pageBuilder)
    {
        columnReader.convertValue(column, pageBuilder);
    }

    public void addColumn(Schema.Builder schemaBuilder)
    {
        schemaBuilder.add(column.getName(), column.getType());
    }
}
