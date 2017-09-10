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

import io.druid.data.input.InputRow;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.plumber.VersioningPolicy;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.IOException;

/**
 * Creates segments based on the realtime configuration spec
 */
public class SpecBasedSegmentAllocator implements SegmentAllocator
{
  private final String dataSource;
  private final VersioningPolicy versioningPolicy;
  private final Granularity segmentGranularity;
  private final ShardSpec shardSpec;

  public SpecBasedSegmentAllocator(DataSchema schema, RealtimeTuningConfig tuningConfig)
  {
    this.dataSource = schema.getDataSource();
    this.versioningPolicy = tuningConfig.getVersioningPolicy();
    this.segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    this.shardSpec = tuningConfig.getShardSpec();
  }

  @Override
  public SegmentIdentifier allocate(
      InputRow row, String sequenceName, String previousSegmentId
  ) throws IOException
  {
    final DateTime intervalStart = segmentGranularity.bucketStart(row.getTimestamp());

    final Interval interval = new Interval(
        intervalStart,
        segmentGranularity.increment(intervalStart)
    );

    String version = versioningPolicy.getVersion(interval);

    return new SegmentIdentifier(dataSource, interval, version, shardSpec);
  }
}
