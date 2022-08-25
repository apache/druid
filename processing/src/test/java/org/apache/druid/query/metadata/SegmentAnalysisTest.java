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

package org.apache.druid.query.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;

public class SegmentAnalysisTest
{
  @Test
  public void testSerde() throws Exception
  {
    // Use LinkedHashMap to preserve order.
    // We'll verify that the order is actually preserved on serde.
    final LinkedHashMap<String, ColumnAnalysis> columns = new LinkedHashMap<>();
    columns.put(
        "b",
        new ColumnAnalysis(ColumnType.LONG, ColumnType.LONG.asTypeString(), true, true, 0, null, null, null, null)
    );
    columns.put(
        "a",
        new ColumnAnalysis(ColumnType.FLOAT, ColumnType.FLOAT.asTypeString(), true, true, 0, null, null, null, null)
    );
    columns.put(
        "f",
        new ColumnAnalysis(ColumnType.STRING, ColumnType.STRING.asTypeString(), true, true, 0, null, null, null, null)
    );
    columns.put(
        "c",
        new ColumnAnalysis(ColumnType.DOUBLE, ColumnType.DOUBLE.asTypeString(), true, true, 0, null, null, null, null)
    );

    final SegmentAnalysis analysis = new SegmentAnalysis(
        "id",
        Intervals.ONLY_ETERNITY,
        columns,
        1,
        2,
        ImmutableMap.of("cnt", new CountAggregatorFactory("cnt")),
        new TimestampSpec(null, null, null),
        Granularities.SECOND,
        true
    );

    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final SegmentAnalysis analysis2 = jsonMapper.readValue(
        jsonMapper.writeValueAsBytes(analysis),
        SegmentAnalysis.class
    );

    Assert.assertEquals(analysis, analysis2);

    // Verify column order is preserved.
    Assert.assertEquals(
        ImmutableList.copyOf(columns.entrySet()),
        ImmutableList.copyOf(analysis2.getColumns().entrySet())
    );
  }
}
