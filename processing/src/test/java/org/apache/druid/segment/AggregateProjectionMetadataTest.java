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
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.projections.AggregateProjectionSchema;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.SortedSet;

class AggregateProjectionMetadataTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @Test
  void testSerde() throws JsonProcessingException
  {
    AggregateProjectionMetadata spec = new AggregateProjectionMetadata(
        new AggregateProjectionSchema(
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
        ),
        12345
    );
    Assertions.assertEquals(
        spec,
        JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(spec), AggregateProjectionMetadata.class)
    );
    String legacy = "{\n"
                    + "  \"type\" : \"aggregate\",\n"
                    + "  \"schema\" : {\n"
                    + "    \"name\" : \"some_projection\",\n"
                    + "    \"timeColumnName\" : \"time\",\n"
                    + "    \"filter\" : {\n"
                    + "      \"type\" : \"equals\",\n"
                    + "      \"column\" : \"a\",\n"
                    + "      \"matchValueType\" : \"STRING\",\n"
                    + "      \"matchValue\" : \"a\"\n"
                    + "    },\n"
                    + "    \"virtualColumns\" : [ {\n"
                    + "      \"type\" : \"expression\",\n"
                    + "      \"name\" : \"time\",\n"
                    + "      \"expression\" : \"timestamp_floor(__time,'PT1H',null,'UTC')\",\n"
                    + "      \"outputType\" : \"LONG\"\n"
                    + "    } ],\n"
                    + "    \"groupingColumns\" : [ \"a\", \"b\", \"time\", \"c\", \"d\" ],\n"
                    + "    \"aggregators\" : [ {\n"
                    + "      \"type\" : \"count\",\n"
                    + "      \"name\" : \"count\"\n"
                    + "    }, {\n"
                    + "      \"type\" : \"longSum\",\n"
                    + "      \"name\" : \"e\",\n"
                    + "      \"fieldName\" : \"e\"\n"
                    + "    } ],\n"
                    + "    \"ordering\" : [ {\n"
                    + "      \"columnName\" : \"a\",\n"
                    + "      \"order\" : \"ascending\"\n"
                    + "    }, {\n"
                    + "      \"columnName\" : \"b\",\n"
                    + "      \"order\" : \"ascending\"\n"
                    + "    }, {\n"
                    + "      \"columnName\" : \"time\",\n"
                    + "      \"order\" : \"ascending\"\n"
                    + "    }, {\n"
                    + "      \"columnName\" : \"c\",\n"
                    + "      \"order\" : \"ascending\"\n"
                    + "    }, {\n"
                    + "      \"columnName\" : \"d\",\n"
                    + "      \"order\" : \"ascending\"\n"
                    + "    } ]\n"
                    + "  },\n"
                    + "  \"numRows\" : 12345\n"
                    + "}";
    Assertions.assertEquals(
        spec,
        JSON_MAPPER.readValue(legacy, AggregateProjectionMetadata.class)
    );
  }

  @Test
  void testComparator()
  {
    SortedSet<AggregateProjectionMetadata> metadataBest = new ObjectAVLTreeSet<>(AggregateProjectionMetadata.COMPARATOR);
    AggregateProjectionMetadata good = new AggregateProjectionMetadata(
        new AggregateProjectionSchema(
            "good",
            "theTime",
            null,
            VirtualColumns.create(Granularities.toVirtualColumn(Granularities.HOUR, "theTime")),
            Arrays.asList("theTime", "a", "b", "c"),
            new AggregatorFactory[]{new CountAggregatorFactory("chocula")},
            Arrays.asList(
                OrderBy.ascending("theTime"),
                OrderBy.ascending("a"),
                OrderBy.ascending("b"),
                OrderBy.ascending("c")
            )
        ),
        123
    );
    // same row count, but less grouping columns aggs more better
    AggregateProjectionMetadata betterLessGroupingColumns = new AggregateProjectionMetadata(
        new AggregateProjectionSchema(
            "betterLessGroupingColumns",
            "theTime",
            null,
            VirtualColumns.create(Granularities.toVirtualColumn(Granularities.HOUR, "theTime")),
            Arrays.asList("c", "d", "theTime"),
            new AggregatorFactory[]{new CountAggregatorFactory("chocula")},
            Arrays.asList(
                OrderBy.ascending("c"),
                OrderBy.ascending("d"),
                OrderBy.ascending("theTime")
            )
        ),
        123
    );
    // same grouping columns, but more aggregators
    AggregateProjectionMetadata evenBetterMoreAggs = new AggregateProjectionMetadata(
        new AggregateProjectionSchema(
            "evenBetterMoreAggs",
            "theTime",
            null,
            VirtualColumns.create(Granularities.toVirtualColumn(Granularities.HOUR, "theTime")),
            Arrays.asList("c", "d", "theTime"),
            new AggregatorFactory[]{
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
        new AggregateProjectionSchema(
            "best",
            null,
            null,
            VirtualColumns.EMPTY,
            Arrays.asList("f", "g"),
            new AggregatorFactory[0],
            Arrays.asList(OrderBy.ascending("f"), OrderBy.ascending("g"))
        ),
        10
    );
    metadataBest.add(good);
    metadataBest.add(betterLessGroupingColumns);
    metadataBest.add(evenBetterMoreAggs);
    metadataBest.add(best);
    Assertions.assertEquals(best, metadataBest.first());
    Assertions.assertArrayEquals(
        new AggregateProjectionMetadata[]{best, evenBetterMoreAggs, betterLessGroupingColumns, good},
        metadataBest.toArray()
    );
  }

  @Test
  void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(AggregateProjectionMetadata.class).usingGetClass().verify();
  }
}
