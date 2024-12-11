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
import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.SortedSet;

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
  public void testComparator()
  {
    SortedSet<AggregateProjectionMetadata> metadataBest = new ObjectAVLTreeSet<>(AggregateProjectionMetadata.COMPARATOR);
    AggregateProjectionMetadata good = new AggregateProjectionMetadata(
        new AggregateProjectionMetadata.Schema(
            "good",
            "theTime",
            VirtualColumns.create(Granularities.toVirtualColumn(Granularities.HOUR, "theTime")),
            Arrays.asList("theTime", "a", "b", "c"),
            new AggregatorFactory[] {
                new CountAggregatorFactory("chocula")
            },
            Arrays.asList(
                OrderBy.ascending("theTime"),
                OrderBy.ascending("a"),
                OrderBy.ascending("b"),
                OrderBy.ascending("c")
            )
        ),
        123
    );
    // same row count, but more aggs more better
    AggregateProjectionMetadata better = new AggregateProjectionMetadata(
        new AggregateProjectionMetadata.Schema(
            "better",
            "theTime",
            VirtualColumns.create(Granularities.toVirtualColumn(Granularities.HOUR, "theTime")),
            Arrays.asList("c", "d", "theTime"),
            new AggregatorFactory[] {
                new CountAggregatorFactory("chocula"),
                new LongSumAggregatorFactory("e", "e")
            },
            Arrays.asList(
                OrderBy.ascending("c"),
                OrderBy.ascending("d"),
                OrderBy.ascending("theTime")
            )
        ),
        123
    );

    // small rows is best
    AggregateProjectionMetadata best = new AggregateProjectionMetadata(
        new AggregateProjectionMetadata.Schema(
            "better",
            null,
            VirtualColumns.EMPTY,
            Arrays.asList("f", "g"),
            new AggregatorFactory[0],
            Arrays.asList(OrderBy.ascending("f"), OrderBy.ascending("g"))
        ),
        10
    );
    metadataBest.add(good);
    metadataBest.add(better);
    metadataBest.add(best);
    Assert.assertEquals(best, metadataBest.first());
    Assert.assertEquals(good, metadataBest.last());
  }

  @Test
  public void testInvalidGrouping()
  {
    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionMetadata(
            new AggregateProjectionMetadata.Schema(
            "other_projection",
            null,
            null,
            null,
            null,
            null
            ),
            0
        )
    );
    Assert.assertEquals("groupingColumns and aggregators must not both be null or empty", t.getMessage());

    t = Assert.assertThrows(
        DruidException.class,
        () -> new AggregateProjectionMetadata(
            new AggregateProjectionMetadata.Schema(
            "other_projection",
            null,
            null,
            Collections.emptyList(),
            null,
            null
            ),
            0
        )
    );
    Assert.assertEquals("groupingColumns and aggregators must not both be null or empty", t.getMessage());
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
                  .withIgnoredFields("orderingWithTimeSubstitution", "timeColumnPosition", "granularity")
                  .usingGetClass()
                  .verify();
  }
}
