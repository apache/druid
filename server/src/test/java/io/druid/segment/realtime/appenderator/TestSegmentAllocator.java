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

package io.druid.segment.realtime.appenderator;

import com.google.common.collect.Maps;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TestSegmentAllocator implements SegmentAllocator
{
  private final String dataSource;
  private final String version;
  private final Granularity granularity;
  private final Map<Long, AtomicInteger> counters = Maps.newHashMap();

  public TestSegmentAllocator(String dataSource, String version, Granularity granularity)
  {
    this.dataSource = dataSource;
    this.version = version;
    this.granularity = granularity;
  }

  @Override
  public SegmentIdentifier allocate(
      final InputRow row,
      final String sequenceName,
      final String previousSegmentId
  ) throws IOException
  {
    synchronized (counters) {
      DateTime dateTimeTruncated = granularity.bucketStart(row.getTimestamp());
      final long timestampTruncated = dateTimeTruncated.getMillis();
      if (!counters.containsKey(timestampTruncated)) {
        counters.put(timestampTruncated, new AtomicInteger());
      }
      final int partitionNum = counters.get(timestampTruncated).getAndIncrement();
      return new SegmentIdentifier(
          dataSource,
          granularity.bucket(dateTimeTruncated),
          version,
          new NumberedShardSpec(partitionNum, 0)
      );
    }
  }
}
