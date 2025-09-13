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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class AggregateProjectionSpecTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @Test
  void testSerde() throws JsonProcessingException
  {
    AggregateProjectionSpec spec = new AggregateProjectionSpec(
        "some_projection",
        new EqualityFilter("a", ColumnType.STRING, "a", null),
        VirtualColumns.create(
            Granularities.toVirtualColumn(Granularities.HOUR, "time")
        ),
        Arrays.asList(
            new StringDimensionSchema("a"),
            new LongDimensionSchema("b"),
            new LongDimensionSchema("time"),
            new FloatDimensionSchema("c"),
            new DoubleDimensionSchema("d")
        ),
        new AggregatorFactory[]{
            new CountAggregatorFactory("count"),
            new LongSumAggregatorFactory("e", "e")
        }
    );
    Assertions.assertEquals(
        spec,
        JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(spec), AggregateProjectionSpec.class)
    );
  }

  @Test
  void testComputeOrdering_noOrdering()
  {
    AggregateProjectionSpec spec = new AggregateProjectionSpec(
        "some_projection",
        null,
        VirtualColumns.EMPTY,
        List.of(),
        new AggregatorFactory[]{
            new CountAggregatorFactory("count"),
            new LongSumAggregatorFactory("e", "e")
        }
    );
    Assertions.assertTrue(spec.getOrdering().isEmpty());
  }

  @Test
  void testComputeOrdering_granularity()
  {
    AggregateProjectionSpec spec = new AggregateProjectionSpec(
        "some_projection",
        null,
        VirtualColumns.EMPTY,
        List.of(new LongDimensionSchema("__time")),
        new AggregatorFactory[]{}
    );
    Assertions.assertEquals("__time", spec.toMetadataSchema().getTimeColumnName());

    ExpressionVirtualColumn hourly = new ExpressionVirtualColumn(
        "hourly",
        "timestamp_floor(__time, 'PT1H', null, null)",
        ColumnType.LONG,
        TestExprMacroTable.INSTANCE
    );
    ExpressionVirtualColumn daily = new ExpressionVirtualColumn(
        "daily",
        "timestamp_floor(__time, 'P1D', null, null)",
        ColumnType.LONG,
        TestExprMacroTable.INSTANCE
    );
    ExpressionVirtualColumn ptEvery10MinLA = new ExpressionVirtualColumn(
        "ptEvery10MinLA",
        "timestamp_floor(__time, 'PT10M', null, 'America/Los_Angeles')",
        ColumnType.LONG,
        TestExprMacroTable.INSTANCE
    );
    ExpressionVirtualColumn ptEvery10Min = new ExpressionVirtualColumn(
        "ptEvery10Min",
        "timestamp_floor(__time, 'PT10M', null, null)",
        ColumnType.LONG,
        TestExprMacroTable.INSTANCE
    );
    ExpressionVirtualColumn every90Min = new ExpressionVirtualColumn(
        "every90Min",
        "timestamp_floor(__time, 'PT1H30M', null, null)",
        ColumnType.LONG,
        TestExprMacroTable.INSTANCE
    );

    Assertions.assertEquals("hourly", new AggregateProjectionSpec(
        "some_projection",
        null,
        VirtualColumns.create(daily, hourly, ptEvery10MinLA),
        List.of(
            new LongDimensionSchema("daily"),
            new LongDimensionSchema("hourly"),
            new LongDimensionSchema("ptEvery10MinLA")
        ),
        new AggregatorFactory[]{}
    ).toMetadataSchema().getTimeColumnName());

    Assertions.assertNull(new AggregateProjectionSpec(
        "some_projection",
        null,
        VirtualColumns.create(ptEvery10MinLA),
        List.of(new LongDimensionSchema("ptEvery10MinLA")),
        new AggregatorFactory[]{}
    ).toMetadataSchema().getTimeColumnName());

    Assertions.assertEquals("every90Min", new AggregateProjectionSpec(
        "some_projection",
        null,
        VirtualColumns.create(every90Min, ptEvery10MinLA),
        List.of(new LongDimensionSchema("every90Min"), new LongDimensionSchema("ptEvery10MinLA")),
        new AggregatorFactory[]{}
    ).toMetadataSchema().getTimeColumnName());

    Assertions.assertEquals("ptEvery10Min", new AggregateProjectionSpec(
        "some_projection",
        null,
        VirtualColumns.create(daily, hourly, ptEvery10Min),
        List.of(
            new LongDimensionSchema("daily"),
            new LongDimensionSchema("hourly"),
            new LongDimensionSchema("ptEvery10Min")
        ),
        new AggregatorFactory[]{}
    ).toMetadataSchema().getTimeColumnName());
    Assertions.assertEquals("hourly", new AggregateProjectionSpec(
        "some_projection",
        null,
        VirtualColumns.create(daily, hourly, ptEvery10Min),
        List.of(
            new LongDimensionSchema("daily"),
            new LongDimensionSchema("hourly"),
            new StringDimensionSchema("ptEvery10Min")
        ),
        new AggregatorFactory[]{}
    ).toMetadataSchema().getTimeColumnName());
  }

  @Test
  void testMissingName()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionSpec(
            null,
            null,
            VirtualColumns.EMPTY,
            List.of(new StringDimensionSchema("string")),
            null
        )
    );

    Assertions.assertEquals("projection name cannot be null or empty", t.getMessage());

    t = Assertions.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionSpec(
            "",
            null,
            VirtualColumns.EMPTY,
            List.of(new StringDimensionSchema("string")),
            null
        )
    );
    Assertions.assertEquals("projection name cannot be null or empty", t.getMessage());
  }

  @Test
  void testInvalidGrouping()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionSpec(
            "other_projection",
            null,
            null,
            null,
            null
        )
    );
    Assertions.assertEquals(
        "projection[other_projection] groupingColumns and aggregators must not both be null or empty",
        t.getMessage()
    );

    t = Assertions.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionSpec(
            "other_projection",
            null,
            null,
            Collections.emptyList(),
            null
        )
    );
    Assertions.assertEquals(
        "projection[other_projection] groupingColumns and aggregators must not both be null or empty",
        t.getMessage()
    );
  }

  @Test
  void testInvalidTimeColumnType()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionSpec(
            "projection",
            null,
            VirtualColumns.EMPTY,
            List.of(new StringDimensionSchema(ColumnHolder.TIME_COLUMN_NAME)),
            null
        )
    );
    Assertions.assertEquals(
        "Encountered grouping column[__time] with incorrect type[STRING]. Type must be 'long'.",
        t.getMessage()
    );
  }

  @Test
  void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(AggregateProjectionSpec.class)
                  .usingGetClass()
                  .withPrefabValues(
                      DimensionSchema.class,
                      new StringDimensionSchema("a"),
                      new DoubleDimensionSchema("d")
                  )
                  .withIgnoredFields("timeColumnName")
                  .verify();
  }
}
