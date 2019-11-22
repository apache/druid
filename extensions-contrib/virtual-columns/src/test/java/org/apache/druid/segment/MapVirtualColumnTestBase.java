/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment;

import com.google.common.io.CharSource;
import org.apache.druid.data.input.impl.DelimitedParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.testing.InitializedNullHandlingTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MapVirtualColumnTestBase extends InitializedNullHandlingTest
{
  static IncrementalIndex generateIndex() throws IOException
  {
    final CharSource input = CharSource.wrap(
        "2011-01-12T00:00:00.000Z\ta\tkey1,key2,key3\tvalue1,value2,value3\n" +
        "2011-01-12T00:00:00.000Z\tb\tkey4,key5,key6\tvalue4\n" +
        "2011-01-12T00:00:00.000Z\tc\tkey1,key5\tvalue1,value5,value9\n"
    );

    final StringInputRowParser parser = new StringInputRowParser(
        new DelimitedParseSpec(
            new TimestampSpec("ts", "auto", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("dim", "keys", "values")), null, null),
            "\t",
            ",",
            Arrays.asList("ts", "dim", "keys", "values"),
            false,
            0
        ),
        "utf8"
    );

    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(DateTimes.of("2011-01-12T00:00:00.000Z").getMillis())
        .build();

    return TestIndex.loadIncrementalIndex(
        () -> new IncrementalIndex.Builder()
            .setIndexSchema(schema)
            .setMaxRowCount(10000)
            .buildOnheap(),
        input,
        parser
    );
  }

  static <K, V> Map<K, V> mapOf(Object... elements)
  {
    final Map<K, V> map = new HashMap<>();
    for (int i = 0; i < elements.length; i += 2) {
      //noinspection unchecked
      map.put((K) elements[i], (V) elements[i + 1]);
    }
    return map;
  }
}
