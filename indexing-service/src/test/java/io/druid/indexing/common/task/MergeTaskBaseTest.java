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

package io.druid.indexing.common.task;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import io.druid.indexing.common.TaskToolbox;
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

  final MergeTaskBase testMergeTaskBase = new MergeTaskBase(null, "foo", segments, null)
  {
    @Override
    protected File merge(TaskToolbox toolbox, Map<DataSegment, File> segments, File outDir) throws Exception
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
    ).toString() + "_";
    Assert.assertEquals(
        desiredPrefix,
        testMergeTaskBase.getId().substring(0, desiredPrefix.length())
    );
  }
}
