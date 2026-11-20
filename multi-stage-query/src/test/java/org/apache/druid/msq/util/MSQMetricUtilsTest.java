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

package org.apache.druid.msq.util;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.msq.input.table.DataServerRequestDescriptor;
import org.apache.druid.msq.input.table.RichSegmentDescriptor;
import org.apache.druid.msq.input.table.SegmentsInputSlice;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

public class MSQMetricUtilsTest
{
  @Test
  public void test_countSegments()
  {
    final SegmentsInputSlice slice1 = new SegmentsInputSlice("foo", List.of(segment(0), segment(1)), null);
    final SegmentsInputSlice slice2 = new SegmentsInputSlice(
        "bar",
        List.of(segment(0)),
        List.of(
            new DataServerRequestDescriptor(Mockito.mock(), List.of(segment(1), segment(2))),
            new DataServerRequestDescriptor(Mockito.mock(), List.of(segment(3)))
        )
    );

    Assertions.assertEquals(6, MSQMetricUtils.countSegments(List.of(slice1, NilInputSlice.INSTANCE, slice2)));
    Assertions.assertEquals(0, MSQMetricUtils.countSegments(List.of(NilInputSlice.INSTANCE)));
  }

  private static RichSegmentDescriptor segment(final int partitionNumber)
  {
    return new RichSegmentDescriptor(Intervals.ETERNITY, Intervals.ETERNITY, "v1", partitionNumber);
  }
}
