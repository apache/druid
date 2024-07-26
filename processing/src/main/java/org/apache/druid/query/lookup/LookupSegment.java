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

package org.apache.druid.query.lookup;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.lookup.LookupColumnSelectorFactory;
import org.apache.druid.timeline.SegmentId;

import java.util.Map;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * A {@link org.apache.druid.segment.Segment} that is based on a {@link LookupExtractor}. Allows direct
 * querying of lookups. The lookup must support {@link LookupExtractor#asMap()}.
 */
public class LookupSegment extends RowBasedSegment<Map.Entry<String, String>>
{
  private static final RowSignature ROW_SIGNATURE =
      RowSignature.builder()
                  .add(LookupColumnSelectorFactory.KEY_COLUMN, ColumnType.STRING)
                  .add(LookupColumnSelectorFactory.VALUE_COLUMN, ColumnType.STRING)
                  .build();

  public LookupSegment(final String lookupName, final LookupExtractorFactory lookupExtractorFactory)
  {
    super(
        SegmentId.dummy(lookupName),
        Sequences.simple(() -> {
          final LookupExtractor extractor = lookupExtractorFactory.get();

          if (!extractor.supportsAsMap()) {
            throw new ISE("Cannot retrieve map view from lookup[%s]", lookupExtractorFactory);
          }

          return extractor.asMap().entrySet().iterator();
        }),
        new RowAdapter<Map.Entry<String, String>>()
        {
          @Override
          public ToLongFunction<Map.Entry<String, String>> timestampFunction()
          {
            // No timestamps for lookups.
            return row -> 0L;
          }

          @Override
          public Function<Map.Entry<String, String>, Object> columnFunction(String columnName)
          {
            if (LookupColumnSelectorFactory.KEY_COLUMN.equals(columnName)) {
              return Map.Entry::getKey;
            } else if (LookupColumnSelectorFactory.VALUE_COLUMN.equals(columnName)) {
              return Map.Entry::getValue;
            } else {
              return row -> null;
            }
          }
        },
        ROW_SIGNATURE
    );
  }
}
