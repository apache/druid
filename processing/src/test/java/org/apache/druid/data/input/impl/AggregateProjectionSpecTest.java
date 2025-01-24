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
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class AggregateProjectionSpecTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    AggregateProjectionSpec spec = new AggregateProjectionSpec(
        "some_projection",
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
        new AggregatorFactory[] {
            new CountAggregatorFactory("count"),
            new LongSumAggregatorFactory("e", "e")
        }
    );
    Assert.assertEquals(spec, JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(spec), AggregateProjectionSpec.class));
  }

  @Test
  public void testInvalidGrouping()
  {
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionSpec(
            "other_projection",
            null,
            null,
            null
        )
    );
    Assert.assertEquals("groupingColumns and aggregators must not both be null or empty", t.getMessage());

    t = Assert.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionSpec(
            "other_projection",
            null,
            Collections.emptyList(),
            null
        )
    );
    Assert.assertEquals("groupingColumns and aggregators must not both be null or empty", t.getMessage());
  }

  @Test
  public void testEqualsAndHashcode()
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
