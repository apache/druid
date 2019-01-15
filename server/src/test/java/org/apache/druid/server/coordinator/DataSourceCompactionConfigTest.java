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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig.UserCompactTuningConfig;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class DataSourceCompactionConfigTest
{
  private static final ObjectMapper objectMapper = new DefaultObjectMapper();

  @Test
  public void testSerdeBasic() throws IOException
  {
    final DataSourceCompactionConfig config = new DataSourceCompactionConfig(
        "dataSource",
        null,
        null,
        500L,
        100L,
        null,
        20,
        new Period(3600),
        null,
        ImmutableMap.of("key", "val")
    );
    final String json = objectMapper.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = objectMapper.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertTrue(fromJson.isKeepSegmentGranularity());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getTargetCompactionSizeBytes(), fromJson.getTargetCompactionSizeBytes());
    Assert.assertEquals(config.getMaxNumSegmentsToCompact(), fromJson.getMaxNumSegmentsToCompact());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
  }

  @Test
  public void testSerdeWithMaxRowsPerSegment() throws IOException
  {
    final DataSourceCompactionConfig config = new DataSourceCompactionConfig(
        "dataSource",
        null,
        null,
        500L,
        null,
        30,
        20,
        new Period(3600),
        null,
        ImmutableMap.of("key", "val")
    );
    final String json = objectMapper.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = objectMapper.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertTrue(fromJson.isKeepSegmentGranularity());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertNull(fromJson.getTargetCompactionSizeBytes());
    Assert.assertEquals(config.getMaxNumSegmentsToCompact(), fromJson.getMaxNumSegmentsToCompact());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
  }

  @Test
  public void testSerdeUserCompactTuningConfig() throws IOException
  {
    final UserCompactTuningConfig config = new UserCompactTuningConfig(null, null, null, null, null);
    final String json = objectMapper.writeValueAsString(config);
    // Check maxRowsPerSegment doesn't exist in the JSON string
    Assert.assertFalse(json.contains("maxRowsPerSegment"));
    final UserCompactTuningConfig fromJson = objectMapper.readValue(json, UserCompactTuningConfig.class);
    Assert.assertEquals(config, fromJson);
  }
}
