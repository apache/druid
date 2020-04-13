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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
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
        new UserCompactionTaskQueryTuningConfig(null, null, null, null, null, null, null, null);
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
        1000,
        10000L,
        2000L,
        new SegmentsSplitHintSpec(42L),
        new IndexSpec(
            new RoaringBitmapSerdeFactory(false),
            CompressionStrategy.LZF,
            CompressionStrategy.UNCOMPRESSED,
            CompressionFactory.LongEncodingStrategy.LONGS
        ),
        1,
        3000L,
        5
    );

    final String json = OBJECT_MAPPER.writeValueAsString(tuningConfig);
    final UserCompactionTaskQueryTuningConfig fromJson =
        OBJECT_MAPPER.readValue(json, UserCompactionTaskQueryTuningConfig.class);
    Assert.assertEquals(tuningConfig, fromJson);
  }
}
