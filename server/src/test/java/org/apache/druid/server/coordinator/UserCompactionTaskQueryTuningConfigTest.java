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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.SegmentsSplitHintSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.BitmapSerde.DefaultBitmapSerdeFactory;
import org.apache.druid.segment.data.CompressionFactory.LongEncodingStrategy;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class UserCompactionTaskQueryTuningConfigTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerdeNulls() throws IOException
  {
    final UserCompactionTaskQueryTuningConfig config =
        new UserCompactionTaskQueryTuningConfig(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    // Check maxRowsPerSegment doesn't exist in the JSON string
    Assert.assertFalse(json.contains("maxRowsPerSegment"));
    final UserCompactionTaskQueryTuningConfig fromJson =
        OBJECT_MAPPER.readValue(json, UserCompactionTaskQueryTuningConfig.class);
    Assert.assertEquals(config, fromJson);
  }

  @Test
  public void testSerde() throws IOException
  {
    final UserCompactionTaskQueryTuningConfig tuningConfig = new UserCompactionTaskQueryTuningConfig(
        40000,
        new OnheapIncrementalIndex.Spec(true),
        2000L,
        null,
        new SegmentsSplitHintSpec(new HumanReadableBytes(42L), null),
        new DynamicPartitionsSpec(1000, 20000L),
        new IndexSpec(
            new DefaultBitmapSerdeFactory(),
            CompressionStrategy.LZ4,
            CompressionStrategy.LZF,
            LongEncodingStrategy.LONGS
        ),
        new IndexSpec(
            new DefaultBitmapSerdeFactory(),
            CompressionStrategy.LZ4,
            CompressionStrategy.UNCOMPRESSED,
            LongEncodingStrategy.AUTO
        ),
        2,
        1000L,
        TmpFileSegmentWriteOutMediumFactory.instance(),
        100,
        5,
        1000L,
        new Duration(3000L),
        7,
        1000,
        100,
        2
    );

    final String json = OBJECT_MAPPER.writeValueAsString(tuningConfig);
    final UserCompactionTaskQueryTuningConfig fromJson =
        OBJECT_MAPPER.readValue(json, UserCompactionTaskQueryTuningConfig.class);
    Assert.assertEquals(tuningConfig, fromJson);
  }
}
