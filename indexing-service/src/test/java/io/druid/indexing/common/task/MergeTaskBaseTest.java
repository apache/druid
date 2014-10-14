/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.indexing.common.task;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

public class MergeTaskBaseTest
{
  private final DataSegment.Builder segmentBuilder = DataSegment.builder()
                                                                .dataSource("foo")
                                                                .version("V1");

  final List<DataSegment> segments = ImmutableList.<DataSegment>builder()
          .add(segmentBuilder.interval(new Interval("2012-01-04/2012-01-06")).build())
          .add(segmentBuilder.interval(new Interval("2012-01-05/2012-01-07")).build())
          .add(segmentBuilder.interval(new Interval("2012-01-03/2012-01-05")).build())
          .build();

  final MergeTaskBase testMergeTaskBase = new MergeTaskBase(null, "foo", segments)
  {
    @Override
    protected File merge(Map<DataSegment, File> segments, File outDir) throws Exception
    {
      return null;
    }

    @Override
    public String getType()
    {
      return "test";
    }
  };

  @Test
  public void testDataSource()
  {
    Assert.assertEquals("foo", testMergeTaskBase.getDataSource());
  }

  @Test
  public void testInterval()
  {
    Assert.assertEquals(new Interval("2012-01-03/2012-01-07"), testMergeTaskBase.getInterval());
  }

  @Test
  public void testID()
  {
    final String desiredPrefix = "merge_foo_" + Hashing.sha1().hashString(
        "2012-01-03T00:00:00.000Z_2012-01-05T00:00:00.000Z_V1_0"
        + "_2012-01-04T00:00:00.000Z_2012-01-06T00:00:00.000Z_V1_0"
        + "_2012-01-05T00:00:00.000Z_2012-01-07T00:00:00.000Z_V1_0"
        , Charsets.UTF_8
    ).toString().toLowerCase() + "_";
    Assert.assertEquals(
        desiredPrefix,
        testMergeTaskBase.getId().substring(0, desiredPrefix.length())
    );
  }
}
