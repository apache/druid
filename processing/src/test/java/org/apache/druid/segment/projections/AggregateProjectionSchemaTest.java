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

package org.apache.druid.segment.projections;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class AggregateProjectionSchemaTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @Test
  void testSerde() throws JsonProcessingException
  {
    AggregateProjectionSchema schema = new AggregateProjectionSchema(
            "some_projection",
            "time",
            new EqualityFilter("a", ColumnType.STRING, "a", null),
            VirtualColumns.create(
                Granularities.toVirtualColumn(Granularities.HOUR, "time")
            ),
            Arrays.asList("a", "b", "time", "c", "d"),
            new AggregatorFactory[]{
                new CountAggregatorFactory("count"),
                new LongSumAggregatorFactory("e", "e")
            },
            Arrays.asList(
                OrderBy.ascending("a"),
                OrderBy.ascending("b"),
                OrderBy.ascending("time"),
                OrderBy.ascending("c"),
                OrderBy.ascending("d")
            )
        );
    Assertions.assertEquals(
        schema,
        JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(schema), AggregateProjectionSchema.class)
    );
  }

  @Test
  void testInvalidName()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionSchema(
            null,
            null,
            null,
            null,
            null,
            new AggregatorFactory[]{new CountAggregatorFactory("count")},
            List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME), OrderBy.ascending("count"))
        )
    );
    Assertions.assertEquals(
        "projection schema name cannot be null or empty",
        t.getMessage()
    );

    t = Assertions.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionSchema(
            "",
            null,
            null,
            null,
            null,
            new AggregatorFactory[]{new CountAggregatorFactory("count")},
            List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME), OrderBy.ascending("count"))
        )
    );
    Assertions.assertEquals(
        "projection schema name cannot be null or empty",
        t.getMessage()
    );
  }

  @Test
  void testInvalidGrouping()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionSchema(
            "other_projection",
            null,
            null,
            null,
            null,
            null,
            null
        )
    );
    Assertions.assertEquals(
        "projection schema[other_projection] groupingColumns and aggregators must not both be null or empty",
        t.getMessage()
    );

    t = Assertions.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionSchema(
            "other_projection",
            null,
            null,
            null,
            Collections.emptyList(),
            null,
            null
        )
    );
    Assertions.assertEquals(
        "projection schema[other_projection] groupingColumns and aggregators must not both be null or empty",
        t.getMessage()
    );
  }

  @Test
  void testInvalidOrdering()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionSchema(
            "no order",
            null,
            null,
            null,
            null,
            new AggregatorFactory[]{new CountAggregatorFactory("count")},
            null
        )
    );
    Assertions.assertEquals(
        "projection schema[no order] ordering must not be null",
        t.getMessage()
    );
  }

  @Test
  void testEqualsAndHashcodeSchema()
  {
    EqualsVerifier.forClass(AggregateProjectionSchema.class)
                  .withIgnoredFields("orderingWithTimeSubstitution", "timeColumnPosition", "effectiveGranularity")
                  .usingGetClass()
                  .verify();
  }
}
