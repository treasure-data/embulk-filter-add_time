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
import org.embulk.filter.add_time.AddTimeFilterPlugin.ToColumnConfig;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;

public class TimestampValueCastConverter
        extends ValueCastConverter
{
    public TimestampValueCastConverter(ToColumnConfig toColumnConfig)
    {
        super(toColumnConfig);
    }

    @Override
    public void convertValue(Column column, final Instant value, final PageBuilder pageBuilder)
    {
        columnVisitor.setValue(value); // it can use the value directly.
        columnVisitor.setPageBuilder(pageBuilder);
        column.visit(columnVisitor);
    }
}
