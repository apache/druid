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

package org.apache.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.metadata.SegmentSchemaCache;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;

import java.io.IOException;

public class SqlSegmentsMetadataManagerTestBase
{
  protected SqlSegmentsMetadataManager sqlSegmentsMetadataManager;
  protected SQLMetadataSegmentPublisher publisher;
  protected SegmentSchemaCache segmentSchemaCache;
  protected SegmentSchemaManager segmentSchemaManager;
  protected TestDerbyConnector connector;
  protected SegmentsMetadataManagerConfig config;
  protected final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

  protected static DataSegment createSegment(
      String dataSource,
      String interval,
      String version,
      String bucketKey,
      int binaryVersion
  )
  {
    return new DataSegment(
        dataSource,
        Intervals.of(interval),
        version,
        ImmutableMap.of(
            "type", "s3_zip",
            "bucket", "test",
            "key", dataSource + "/" + bucketKey
        ),
        ImmutableList.of("dim1", "dim2", "dim3"),
        ImmutableList.of("count", "value"),
        NoneShardSpec.instance(),
        binaryVersion,
        1234L
    );
  }

  protected final DataSegment segment1 = createSegment(
      "wikipedia",
      "2012-03-15T00:00:00.000/2012-03-16T00:00:00.000",
      "2012-03-16T00:36:30.848Z",
      "index/y=2012/m=03/d=15/2012-03-16T00:36:30.848Z/0/index.zip",
      0
  );

  protected final DataSegment segment2 = createSegment(
      "wikipedia",
      "2012-01-05T00:00:00.000/2012-01-06T00:00:00.000",
      "2012-01-06T22:19:12.565Z",
      "wikipedia/index/y=2012/m=01/d=05/2012-01-06T22:19:12.565Z/0/index.zip",
      0
  );

  protected void publish(DataSegment segment, boolean used) throws IOException
  {
    publish(segment, used, DateTimes.nowUtc());
  }

  protected void publish(DataSegment segment, boolean used, DateTime usedFlagLastUpdated) throws IOException
  {
    boolean partitioned = !(segment.getShardSpec() instanceof NoneShardSpec);

    String usedFlagLastUpdatedStr = null;
    if (null != usedFlagLastUpdated) {
      usedFlagLastUpdatedStr = usedFlagLastUpdated.toString();
    }
    publisher.publishSegment(
        segment.getId().toString(),
        segment.getDataSource(),
        DateTimes.nowUtc().toString(),
        segment.getInterval().getStart().toString(),
        segment.getInterval().getEnd().toString(),
        partitioned,
        segment.getVersion(),
        used,
        jsonMapper.writeValueAsBytes(segment),
        usedFlagLastUpdatedStr
    );
  }
}
