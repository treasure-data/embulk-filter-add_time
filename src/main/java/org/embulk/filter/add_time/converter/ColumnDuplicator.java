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

public class ColumnDuplicator
        implements ColumnConverter
{
    public static class Builder
    {
        private Column column;
        private ColumnReader duplicator;
        private ColumnReader duplicatee;

        public Builder()
        {
        }

        public Builder setColumn(Column column)
        {
            this.column = column;
            return this;
        }

        public Builder setDuplicator(ColumnReader duplicator)
        {
            this.duplicator = duplicator;
            return this;
        }

        public Builder setDuplicatee(ColumnReader duplicatee)
        {
            this.duplicatee = duplicatee;
            return this;
        }

        public ColumnDuplicator build()
        {
            return new ColumnDuplicator(column, duplicator, duplicatee);
        }
    }

    private final Column column;
    private final ColumnReader duplicator;
    private final ColumnReader duplicatee;

    private ColumnDuplicator(Column column, ColumnReader duplicator, ColumnReader duplicatee)
    {
        this.column = column;
        this.duplicator = duplicator;
        this.duplicatee = duplicatee;
    }

    public void update(PageReader pageReader)
    {
        duplicator.readValue(column, pageReader);
        duplicator.copyTo(duplicatee);
    }

    public void convert(PageBuilder pageBuilder)
    {
        duplicator.convertValue(column, pageBuilder);
    }

    public void addColumn(Schema.Builder schemaBuilder)
    {
        schemaBuilder.add(column.getName(), column.getType());
    }
}
