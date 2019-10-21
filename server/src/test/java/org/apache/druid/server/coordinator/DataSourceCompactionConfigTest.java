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
import org.apache.druid.data.input.SegmentsSplitHintSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionFactory.LongEncodingStrategy;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig.UserCompactTuningConfig;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public class DataSourceCompactionConfigTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerdeBasic() throws IOException
  {
    final DataSourceCompactionConfig config = new DataSourceCompactionConfig(
        "dataSource",
        null,
        500L,
        null,
        new Period(3600),
        null,
        ImmutableMap.of("key", "val")
    );
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getMaxRowsPerSegment(), fromJson.getMaxRowsPerSegment());
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
        500L,
        30,
        new Period(3600),
        null,
        ImmutableMap.of("key", "val")
    );
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getMaxRowsPerSegment(), fromJson.getMaxRowsPerSegment());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
  }

  @Test
  public void testSerdeUserCompactTuningConfig() throws IOException
  {
    final UserCompactTuningConfig config = new UserCompactTuningConfig(null, null, null, null, null, null, null, null);
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    // Check maxRowsPerSegment doesn't exist in the JSON string
    Assert.assertFalse(json.contains("maxRowsPerSegment"));
    final UserCompactTuningConfig fromJson = OBJECT_MAPPER.readValue(json, UserCompactTuningConfig.class);
    Assert.assertEquals(config, fromJson);
  }

  @Test
  public void testSerdeWithMaxTotalRows() throws IOException
  {
    final DataSourceCompactionConfig config = new DataSourceCompactionConfig(
        "dataSource",
        null,
        500L,
        null,
        new Period(3600),
        new UserCompactTuningConfig(
            null,
            null,
            10000L,
            null,
            null,
            null,
            null,
            null
        ),
        ImmutableMap.of("key", "val")
    );
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getMaxRowsPerSegment(), fromJson.getMaxRowsPerSegment());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
  }

  @Test
  public void testSerdeMaxTotalRowsWithMaxRowsPerSegment() throws IOException
  {
    final DataSourceCompactionConfig config = new DataSourceCompactionConfig(
        "dataSource",
        null,
        500L,
        10000,
        new Period(3600),
        new UserCompactTuningConfig(
            null,
            null,
            10000L,
            null,
            null,
            null,
            null,
            null
        ),
        ImmutableMap.of("key", "val")
    );

    final String json = OBJECT_MAPPER.writeValueAsString(config);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertEquals(config.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(25, fromJson.getTaskPriority());
    Assert.assertEquals(config.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(config.getMaxRowsPerSegment(), fromJson.getMaxRowsPerSegment());
    Assert.assertEquals(config.getSkipOffsetFromLatest(), fromJson.getSkipOffsetFromLatest());
    Assert.assertEquals(config.getTuningConfig(), fromJson.getTuningConfig());
    Assert.assertEquals(config.getTaskContext(), fromJson.getTaskContext());
  }

  @Test
  public void testSerdeUserCompactionTuningConfig() throws IOException
  {
    final UserCompactTuningConfig tuningConfig = new UserCompactTuningConfig(
        1000,
        10000L,
        2000L,
        new SegmentsSplitHintSpec(10000L),
        new IndexSpec(
            new RoaringBitmapSerdeFactory(false),
            CompressionStrategy.LZF,
            CompressionStrategy.UNCOMPRESSED,
            LongEncodingStrategy.LONGS
        ),
        1,
        3000L,
        null
    );

    final String json = OBJECT_MAPPER.writeValueAsString(tuningConfig);
    final UserCompactTuningConfig fromJson = OBJECT_MAPPER.readValue(json, UserCompactTuningConfig.class);
    Assert.assertEquals(tuningConfig, fromJson);
  }
}
