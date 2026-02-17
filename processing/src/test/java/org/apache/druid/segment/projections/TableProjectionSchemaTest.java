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
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class TableProjectionSchemaTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @Test
  void testSerde() throws JsonProcessingException
  {
    TableProjectionSchema schema = new TableProjectionSchema(
        VirtualColumns.EMPTY,
        List.of(ColumnHolder.TIME_COLUMN_NAME, "a", "b"),
        new AggregatorFactory[]{
            new CountAggregatorFactory("count"),
            new LongSumAggregatorFactory("c", "c")
        },
        List.of(
            OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME),
            OrderBy.ascending("a"),
            OrderBy.ascending("b")
        )
    );

    Assertions.assertEquals(
        schema,
        JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(schema), ProjectionSchema.class)
    );
  }

  @Test
  void testInvalidGrouping()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new TableProjectionSchema(
            null,
            null,
            null,
            null
        )
    );
    Assertions.assertEquals(
        "base table projection schema columns must not be null or empty",
        t.getMessage()
    );

    t = Assertions.assertThrows(
        DruidException.class,
        () -> new TableProjectionSchema(
            null,
            List.of(),
            null,
            null
        )
    );
    Assertions.assertEquals(
        "base table projection schema columns must not be null or empty",
        t.getMessage()
    );
  }

  @Test
  void testInvalidOrdering()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> new TableProjectionSchema(
            null,
            List.of(ColumnHolder.TIME_COLUMN_NAME),
            new AggregatorFactory[]{new CountAggregatorFactory("count")},
            null
        )
    );
    Assertions.assertEquals(
        "base table projection schema ordering must not be null",
        t.getMessage()
    );
  }

  @Test
  void testEqualsAndHashcodeSchema()
  {
    EqualsVerifier.forClass(TableProjectionSchema.class)
                  .withIgnoredFields("timeColumnPosition", "effectiveGranularity")
                  .usingGetClass()
                  .verify();
  }
}
