/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 */
public class MetadataTest
{
  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

    AggregatorFactory[] aggregators = new AggregatorFactory[] {
        new LongSumAggregatorFactory("out", "in")
    };

    Metadata metadata = new Metadata(
        Collections.singletonMap("k", "v"),
        aggregators,
        null,
        Granularities.ALL,
        Boolean.FALSE
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
    Assert.assertNull(Metadata.merge(ImmutableList.<Metadata>of(), null));

    List<Metadata> metadataToBeMerged = new ArrayList<>();

    metadataToBeMerged.add(null);
    Assert.assertNull(Metadata.merge(metadataToBeMerged, null));

    //sanity merge check
    AggregatorFactory[] aggs = new AggregatorFactory[] {
        new LongMaxAggregatorFactory("n", "f")
    };
    final Metadata m1 = new Metadata(
        Collections.singletonMap("k", "v"),
        aggs,
        new TimestampSpec("ds", "auto", null),
        Granularities.ALL,
        Boolean.FALSE
    );

    final Metadata m2 = new Metadata(
        Collections.singletonMap("k", "v"),
        aggs,
        new TimestampSpec("ds", "auto", null),
        Granularities.ALL,
        Boolean.FALSE
    );

    final Metadata m3 = new Metadata(
        Collections.singletonMap("k", "v"),
        aggs,
        new TimestampSpec("ds", "auto", null),
        Granularities.ALL,
        Boolean.TRUE
    );

    final Metadata merged = new Metadata(
        Collections.singletonMap("k", "v"),
        new AggregatorFactory[]{
            new LongMaxAggregatorFactory("n", "n")
        },
        new TimestampSpec("ds", "auto", null),
        Granularities.ALL,
        Boolean.FALSE
    );
    Assert.assertEquals(merged, Metadata.merge(ImmutableList.of(m1, m2), null));

    //merge check with one metadata being null
    metadataToBeMerged.clear();
    metadataToBeMerged.add(m1);
    metadataToBeMerged.add(m2);
    metadataToBeMerged.add(null);

    final Metadata merged2 = new Metadata(Collections.singletonMap("k", "v"), null, null, null, null);

    Assert.assertEquals(merged2, Metadata.merge(metadataToBeMerged, null));

    //merge check with client explicitly providing merged aggregators
    AggregatorFactory[] explicitAggs = new AggregatorFactory[] {
        new DoubleMaxAggregatorFactory("x", "y")
    };

    final Metadata merged3 = new Metadata(Collections.singletonMap("k", "v"), explicitAggs, null, null, null);

    Assert.assertEquals(
        merged3,
        Metadata.merge(metadataToBeMerged, explicitAggs)
    );

    final Metadata merged4 = new Metadata(
        Collections.singletonMap("k", "v"),
        explicitAggs,
        new TimestampSpec("ds", "auto", null),
        Granularities.ALL,
        null
    );
    Assert.assertEquals(
        merged4,
        Metadata.merge(ImmutableList.of(m3, m2), explicitAggs)
    );
  }
}
