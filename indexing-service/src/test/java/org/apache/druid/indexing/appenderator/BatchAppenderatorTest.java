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

package org.apache.druid.indexing.appenderator;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorTester;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class BatchAppenderatorTest extends InitializedNullHandlingTest
{
  private static final List<SegmentIdWithShardSpec> IDENTIFIERS = ImmutableList.of(
      si("2000/2001", "A", 0),
      si("2000/2001", "A", 1),
      si("2001/2002", "A", 0)
  );

  @Test
  public void testSimpleIngestion() throws Exception
  {
    try (final BatchAppenderatorTester tester = new BatchAppenderatorTester(2, true)) {
      final Appenderator appenderator = tester.getAppenderator();
      boolean thrown;

      // startJob
      Assert.assertEquals(null, appenderator.startJob());

      // getDataSource
      Assert.assertEquals(AppenderatorTester.DATASOURCE, appenderator.getDataSource());

      // add
      Assert.assertEquals(
          1,
          appenderator.add(IDENTIFIERS.get(0), ir("2000", "foo", 1), null)
                      .getNumRowsInSegment()
      );

      Assert.assertEquals(
          2,
          appenderator.add(IDENTIFIERS.get(0), ir("2000", "bar", 2), null)
                      .getNumRowsInSegment()
      );

      Assert.assertEquals(
          1,
          appenderator.add(IDENTIFIERS.get(1), ir("2000", "qux", 4), null)
                      .getNumRowsInSegment()
      );

      // getSegments
      Assert.assertEquals(IDENTIFIERS.subList(0, 2), sorted(appenderator.getSegments()));

      // getRowCount
      Assert.assertEquals(2, appenderator.getRowCount(IDENTIFIERS.get(0)));
      Assert.assertEquals(1, appenderator.getRowCount(IDENTIFIERS.get(1)));
      thrown = false;
      try {
        appenderator.getRowCount(IDENTIFIERS.get(2));
      }
      catch (IllegalStateException e) {
        thrown = true;
      }
      Assert.assertTrue(thrown);

      // push all
      final SegmentsAndCommitMetadata segmentsAndCommitMetadata = appenderator.push(
          appenderator.getSegments(),
          null,
          false
      ).get();
      Assert.assertEquals(
          IDENTIFIERS.subList(0, 2),
          sorted(
              Lists.transform(
                  segmentsAndCommitMetadata.getSegments(),
                  new Function<DataSegment, SegmentIdWithShardSpec>()
                  {
                    @Override
                    public SegmentIdWithShardSpec apply(DataSegment input)
                    {
                      return SegmentIdWithShardSpec.fromDataSegment(input);
                    }
                  }
              )
          )
      );
      Assert.assertEquals(sorted(tester.getPushedSegments()), sorted(segmentsAndCommitMetadata.getSegments()));

      // clear
      appenderator.clear();
      Assert.assertTrue(appenderator.getSegments().isEmpty());
    }
  }

  private static SegmentIdWithShardSpec si(String interval, String version, int partitionNum)
  {
    return new SegmentIdWithShardSpec(
        AppenderatorTester.DATASOURCE,
        Intervals.of(interval),
        version,
        new LinearShardSpec(partitionNum)
    );
  }

  static InputRow ir(String ts, String dim, Object met)
  {
    return new MapBasedInputRow(
        DateTimes.of(ts).getMillis(),
        ImmutableList.of("dim"),
        ImmutableMap.of(
            "dim",
            dim,
            "met",
            met
        )
    );
  }

  private static <T> List<T> sorted(final List<T> xs)
  {
    final List<T> xsSorted = Lists.newArrayList(xs);
    Collections.sort(
        xsSorted,
        (T a, T b) -> {
          if (a instanceof SegmentIdWithShardSpec && b instanceof SegmentIdWithShardSpec) {
            return ((SegmentIdWithShardSpec) a).compareTo(((SegmentIdWithShardSpec) b));
          } else if (a instanceof DataSegment && b instanceof DataSegment) {
            return ((DataSegment) a).getId().compareTo(((DataSegment) b).getId());
          } else {
            throw new IllegalStateException("BAD");
          }
        }
    );
    return xsSorted;
  }

}

