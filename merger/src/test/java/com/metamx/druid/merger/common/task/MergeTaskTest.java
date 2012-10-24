/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.merger.common.task;

import com.google.common.collect.ImmutableList;
import com.metamx.druid.client.DataSegment;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

public class MergeTaskTest
{
  final List<DataSegment> segments =
      ImmutableList
          .<DataSegment>builder()
          .add(new DataSegment("foo", new Interval("2012-01-04/2012-01-06"), "V1", null, null, null, null, -1))
          .add(new DataSegment("foo", new Interval("2012-01-05/2012-01-07"), "V1", null, null, null, null, -1))
          .add(new DataSegment("foo", new Interval("2012-01-03/2012-01-05"), "V1", null, null, null, null, -1))
          .build();

  final MergeTask testMergeTask = new MergeTask("foo", segments)
  {
    @Override
    protected File merge(Map<DataSegment, File> segments, File outDir) throws Exception
    {
      return null;
    }

    @Override
    public Type getType()
    {
      return Type.TEST;
    }
  };

  @Test
  public void testDataSource()
  {
    Assert.assertEquals("foo", testMergeTask.getDataSource());
  }

  @Test
  public void testInterval()
  {
    Assert.assertEquals(new Interval("2012-01-03/2012-01-07"), testMergeTask.getInterval());
  }

  @Test
  public void testID()
  {
    final String desiredPrefix = "merge_foo_" + DigestUtils.shaHex(
        "2012-01-03T00:00:00.000Z_2012-01-05T00:00:00.000Z_V1_0"
        + "_2012-01-04T00:00:00.000Z_2012-01-06T00:00:00.000Z_V1_0"
        + "_2012-01-05T00:00:00.000Z_2012-01-07T00:00:00.000Z_V1_0"
    ) + "_";
    Assert.assertEquals(
        desiredPrefix,
        testMergeTask.getId().substring(0, desiredPrefix.length())
    );
  }
}
