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
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.msgpack.value.Value;

public interface ValueConverter
{
    ValueConverter NO_CONV = new ValueNoConverter();

    void convertNull(Column column, PageBuilder pageBuilder);

    void convertValue(Column column, boolean value, PageBuilder pageBuilder);

    void convertValue(Column column, long value, PageBuilder pageBuilder);

    void convertValue(Column column, double value, PageBuilder pageBuilder);

    void convertValue(Column column, String value, PageBuilder pageBuilder);

    void convertValue(Column column, Value value, PageBuilder pageBuilder);

    void convertValue(Column column, Instant value, PageBuilder pageBuilder);
}
