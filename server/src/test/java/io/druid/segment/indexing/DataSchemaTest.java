/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
                new TimestampSpec("time", "auto"),
                new DimensionsSpec(ImmutableList.of("dimB", "dimA"), null, null)
            ),
            null, null, null, null
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
                new TimestampSpec("time", "auto"),
                new DimensionsSpec(ImmutableList.of("time", "dimA", "dimB", "col2"), ImmutableList.of("dimC"), null)
            ),
            null, null, null, null
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
