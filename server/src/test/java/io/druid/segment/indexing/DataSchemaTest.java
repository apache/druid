/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.junit.Assert;
import org.joda.time.Interval;
import org.junit.Test;

public class DataSchemaTest
{
  @Test
  public void testDefaultExclusions() throws Exception
  {
    DataSchema schema = new DataSchema(
        "test",
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("time", "auto", null),
                new DimensionsSpec(ImmutableList.of("dimB", "dimA"), null, null)
            )
        ),
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2"),
        },
        new ArbitraryGranularitySpec(QueryGranularity.DAY, ImmutableList.of(Interval.parse("2014/2015")))
    );

    Assert.assertEquals(
        ImmutableSet.of("time", "col1", "col2"),
        schema.getParser().getParseSpec().getDimensionsSpec().getDimensionExclusions()
    );
  }

  @Test
  public void testExplicitInclude() throws Exception
  {
    DataSchema schema = new DataSchema(
        "test",
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("time", "auto", null),
                new DimensionsSpec(ImmutableList.of("time", "dimA", "dimB", "col2"), ImmutableList.of("dimC"), null)
            )
        ),
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2"),
        },
        new ArbitraryGranularitySpec(QueryGranularity.DAY, ImmutableList.of(Interval.parse("2014/2015")))
    );

    Assert.assertEquals(
        ImmutableSet.of("dimC", "col1"),
        schema.getParser().getParseSpec().getDimensionsSpec().getDimensionExclusions()
    );
  }
}
