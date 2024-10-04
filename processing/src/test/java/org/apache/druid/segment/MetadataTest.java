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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.firstlast.last.LongLastAggregatorFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class MetadataTest extends InitializedNullHandlingTest
{
  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

    AggregatorFactory[] aggregators = new AggregatorFactory[]{
        new LongSumAggregatorFactory("out", "in")
    };

    Metadata metadata = new Metadata(
        Collections.singletonMap("k", "v"),
        aggregators,
        null,
        Granularities.ALL,
        Boolean.FALSE,
        null,
        null
    );

    Metadata other = jsonMapper.readValue(
        jsonMapper.writeValueAsString(metadata),
        Metadata.class
    );

    Assert.assertEquals(metadata, other);
  }

  @Test
  public void testMerge()
  {
    Assert.assertNull(Metadata.merge(null, null));
    Assert.assertNull(Metadata.merge(ImmutableList.of(), null));

    List<Metadata> metadataToBeMerged = new ArrayList<>();

    metadataToBeMerged.add(null);
    Assert.assertNull(Metadata.merge(metadataToBeMerged, null));

    //sanity merge check
    AggregatorFactory[] aggs = new AggregatorFactory[]{
        new LongMaxAggregatorFactory("n", "f")
    };
    List<AggregateProjectionMetadata> projectionSpecs = ImmutableList.of(
        new AggregateProjectionMetadata(
            new AggregateProjectionMetadata.Schema(
                "some_projection",
                "__gran",
                VirtualColumns.create(
                    Granularities.toVirtualColumn(Granularities.HOUR, "__gran")
                ),
                Arrays.asList("a", "b", "__gran"),
                new AggregatorFactory[]{
                    new LongLastAggregatorFactory("atLongLast", "d", null)
                },
                makeOrderBy("a", "b", "__gran")
            ),
            1234
        )
    );

    final Metadata m1 = new Metadata(
        Collections.singletonMap("k", "v"),
        aggs,
        new TimestampSpec("ds", "auto", null),
        Granularities.ALL,
        Boolean.FALSE,
        null,
        projectionSpecs
    );

    final Metadata m2 = new Metadata(
        Collections.singletonMap("k", "v"),
        aggs,
        new TimestampSpec("ds", "auto", null),
        Granularities.ALL,
        Boolean.FALSE,
        null,
        projectionSpecs
    );

    final Metadata m3 = new Metadata(
        Collections.singletonMap("k", "v"),
        aggs,
        new TimestampSpec("ds", "auto", null),
        Granularities.ALL,
        Boolean.TRUE,
        null,
        projectionSpecs
    );

    final Metadata merged = new Metadata(
        Collections.singletonMap("k", "v"),
        new AggregatorFactory[]{
            new LongMaxAggregatorFactory("n", "n")
        },
        new TimestampSpec("ds", "auto", null),
        Granularities.ALL,
        Boolean.FALSE,
        Cursors.ascendingTimeOrder(),
        projectionSpecs
    );
    Assert.assertEquals(merged, Metadata.merge(ImmutableList.of(m1, m2), null));

    //merge check with one metadata being null
    metadataToBeMerged.clear();
    metadataToBeMerged.add(m1);
    metadataToBeMerged.add(m2);
    metadataToBeMerged.add(null);

    final Metadata merged2 =
        new Metadata(
            Collections.singletonMap("k", "v"),
            null,
            null,
            null,
            null,
            Cursors.ascendingTimeOrder(),
            projectionSpecs
        );

    Assert.assertEquals(merged2, Metadata.merge(metadataToBeMerged, null));

    //merge check with client explicitly providing merged aggregators
    AggregatorFactory[] explicitAggs = new AggregatorFactory[]{
        new DoubleMaxAggregatorFactory("x", "y")
    };

    final Metadata merged3 =
        new Metadata(
            Collections.singletonMap("k", "v"),
            explicitAggs,
            null,
            null,
            null,
            Cursors.ascendingTimeOrder(),
            projectionSpecs
        );

    Assert.assertEquals(
        merged3,
        Metadata.merge(metadataToBeMerged, explicitAggs)
    );

    final Metadata merged4 = new Metadata(
        Collections.singletonMap("k", "v"),
        explicitAggs,
        new TimestampSpec("ds", "auto", null),
        Granularities.ALL,
        null,
        Cursors.ascendingTimeOrder(),
        projectionSpecs
    );
    Assert.assertEquals(
        merged4,
        Metadata.merge(ImmutableList.of(m3, m2), explicitAggs)
    );
  }

  @Test
  public void testMergeOrderings()
  {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> Metadata.mergeOrderings(Collections.emptyList())
    );

    Assert.assertEquals(
        Cursors.ascendingTimeOrder(),
        Metadata.mergeOrderings(Collections.singletonList(null))
    );

    Assert.assertEquals(
        Collections.emptyList(),
        Metadata.mergeOrderings(Arrays.asList(null, makeOrderBy("foo", "bar")))
    );

    Assert.assertEquals(
        Collections.emptyList(),
        Metadata.mergeOrderings(Arrays.asList(makeOrderBy("foo", "bar"), null))
    );

    Assert.assertEquals(
        Cursors.ascendingTimeOrder(),
        Metadata.mergeOrderings(Arrays.asList(makeOrderBy("__time", "foo", "bar"), null))
    );

    Assert.assertEquals(
        Collections.emptyList(),
        Metadata.mergeOrderings(
            Arrays.asList(
                makeOrderBy("foo", "bar"),
                makeOrderBy("bar", "foo")
            )
        )
    );

    Assert.assertEquals(
        Collections.singletonList(OrderBy.ascending("bar")),
        Metadata.mergeOrderings(
            Arrays.asList(
                makeOrderBy("bar", "baz"),
                makeOrderBy("bar", "foo")
            )
        )
    );

    Assert.assertEquals(
        ImmutableList.of(OrderBy.ascending("bar"), OrderBy.ascending("foo")),
        Metadata.mergeOrderings(
            Arrays.asList(
                makeOrderBy("bar", "foo"),
                makeOrderBy("bar", "foo")
            )
        )
    );
  }

  @Test
  public void testMergeProjectionsUnexpectedMismatch()
  {
    List<AggregateProjectionMetadata> p1 = ImmutableList.of(
        new AggregateProjectionMetadata(
            new AggregateProjectionMetadata.Schema(
                "some_projection",
                "__gran",
                VirtualColumns.create(
                    Granularities.toVirtualColumn(Granularities.HOUR, "__gran")
                ),
                Arrays.asList("a", "b", "__gran"),
                new AggregatorFactory[]{
                    new LongLastAggregatorFactory("atLongLast", "d", null)
                },
                makeOrderBy("a", "b", "__gran")
            ),
            654321
        )
    );

    List<AggregateProjectionMetadata> p2 = ImmutableList.of(
        new AggregateProjectionMetadata(
            new AggregateProjectionMetadata.Schema(
                "some_projection",
                "__gran",
                VirtualColumns.create(
                    Granularities.toVirtualColumn(Granularities.HOUR, "__gran")
                ),
                Arrays.asList("a", "b", "_gran"),
                new AggregatorFactory[]{
                    new LongSumAggregatorFactory("longSum", "d")
                },
                makeOrderBy("a", "b", "__gran")
            ),
            1234
        )
    );

    List<AggregateProjectionMetadata> p3 = ImmutableList.of(
        new AggregateProjectionMetadata(
            new AggregateProjectionMetadata.Schema(
                "some_projection",
                "__gran",
                VirtualColumns.create(
                    Granularities.toVirtualColumn(Granularities.HOUR, "__gran")
                ),
                Arrays.asList("a", "b", "__gran"),
                new AggregatorFactory[]{
                    new LongLastAggregatorFactory("atLongLast", "d", null)
                },
                makeOrderBy("a", "b", "__gran")
            ),
            12121
        ),
        new AggregateProjectionMetadata(
            new AggregateProjectionMetadata.Schema(
                "some_projection2",
                "__gran",
                VirtualColumns.create(
                    Granularities.toVirtualColumn(Granularities.DAY, "__gran")
                ),
                Arrays.asList("__gran", "a"),
                new AggregatorFactory[]{
                    new LongSumAggregatorFactory("longSum", "d")
                },
                makeOrderBy("__gran", "a")
            ),
            555
        )
    );

    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> Metadata.validateProjections(Arrays.asList(p1, p2))
    );
    MatcherAssert.assertThat(
        t.getMessage(),
        CoreMatchers.startsWith("Unable to merge projections: mismatched projections")
    );

    t = Assert.assertThrows(
        DruidException.class,
        () -> Metadata.validateProjections(Arrays.asList(p1, p3))
    );

    MatcherAssert.assertThat(
        t.getMessage(),
        CoreMatchers.startsWith("Unable to merge projections: mismatched projections count")
    );

    t = Assert.assertThrows(
        DruidException.class,
        () -> Metadata.validateProjections(Arrays.asList(p1, null))
    );
    MatcherAssert.assertThat(
        t.getMessage(),
        CoreMatchers.startsWith("Unable to merge projections: some projections were null")
    );
  }

  private static List<OrderBy> makeOrderBy(final String... columnNames)
  {
    return Arrays.stream(columnNames).map(OrderBy::ascending).collect(Collectors.toList());
  }
}
