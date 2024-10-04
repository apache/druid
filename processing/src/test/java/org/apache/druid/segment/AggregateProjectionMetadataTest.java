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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class AggregateProjectionMetadataTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    AggregateProjectionMetadata spec = new AggregateProjectionMetadata(
        new AggregateProjectionMetadata.Schema(
            "some_projection",
            "time",
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
        ),
        12345
    );
    Assert.assertEquals(
        spec,
        JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(spec), AggregateProjectionMetadata.class)
    );
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(AggregateProjectionMetadata.class).usingGetClass().verify();
  }

  @Test
  public void testEqualsAndHashcodeSchema()
  {
    EqualsVerifier.forClass(AggregateProjectionMetadata.Schema.class)
                  .withIgnoredFields("orderingWithTimeSubstitution", "timePosition", "graularity")
                  .usingGetClass()
                  .verify();
  }
}