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

package org.apache.druid.indexing.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorTuningConfig;
import org.apache.druid.indexing.kafka.test.TestModifiedKafkaIndexTaskTuningConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.indexing.TuningConfig;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

public class KafkaIndexTaskTuningConfigTest
{
  private final ObjectMapper mapper;

  public KafkaIndexTaskTuningConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules(new KafkaIndexTaskModule().getJacksonModules());
  }

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String jsonStr = "{\"type\": \"kafka\"}";

    KafkaIndexTaskTuningConfig config = (KafkaIndexTaskTuningConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                TuningConfig.class
            )
        ),
        TuningConfig.class
    );

    Assert.assertNull(config.getBasePersistDirectory());
    Assert.assertEquals(new OnheapIncrementalIndex.Spec(), config.getAppendableIndexSpec());
    Assert.assertEquals(150000, config.getMaxRowsInMemory());
    Assert.assertEquals(5_000_000, config.getMaxRowsPerSegment().intValue());
    Assert.assertNull(config.getMaxTotalRows());
    Assert.assertEquals(new Period("PT10M"), config.getIntermediatePersistPeriod());
    Assert.assertEquals(0, config.getMaxPendingPersists());
    Assert.assertEquals(IndexSpec.DEFAULT, config.getIndexSpec());
    Assert.assertEquals(IndexSpec.DEFAULT, config.getIndexSpecForIntermediatePersists());
    Assert.assertEquals(false, config.isReportParseExceptions());
    Assert.assertEquals(Duration.ofMinutes(15).toMillis(), config.getHandoffConditionTimeout());
    Assert.assertEquals(1, config.getNumPersistThreads());
  }

  @Test
  public void testSerdeWithNonDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"basePersistDirectory\": \"/tmp/xxx\",\n"
                     + "  \"maxRowsInMemory\": 100,\n"
                     + "  \"maxRowsPerSegment\": 100,\n"
                     + "  \"maxTotalRows\": 1000,\n"
                     + "  \"intermediatePersistPeriod\": \"PT1H\",\n"
                     + "  \"maxPendingPersists\": 100,\n"
                     + "  \"reportParseExceptions\": true,\n"
                     + "  \"handoffConditionTimeout\": 100,\n"
                     + "  \"indexSpec\": { \"metricCompression\" : \"NONE\" },\n"
                     + "  \"indexSpecForIntermediatePersists\": { \"dimensionCompression\" : \"uncompressed\" },\n"
                     + "  \"appendableIndexSpec\": { \"type\" : \"onheap\" },\n"
                     + "  \"numPersistThreads\": 2\n"
                     + "}";

    KafkaIndexTaskTuningConfig config = (KafkaIndexTaskTuningConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                TuningConfig.class
            )
        ),
        TuningConfig.class
    );

    Assert.assertNull(config.getBasePersistDirectory());
    Assert.assertEquals(new OnheapIncrementalIndex.Spec(), config.getAppendableIndexSpec());
    Assert.assertEquals(100, config.getMaxRowsInMemory());
    Assert.assertEquals(100, config.getMaxRowsPerSegment().intValue());
    Assert.assertNotEquals(null, config.getMaxTotalRows());
    Assert.assertEquals(1000, config.getMaxTotalRows().longValue());
    Assert.assertEquals(new Period("PT1H"), config.getIntermediatePersistPeriod());
    Assert.assertEquals(100, config.getMaxPendingPersists());
    Assert.assertEquals(true, config.isReportParseExceptions());
    Assert.assertEquals(100, config.getHandoffConditionTimeout());
    Assert.assertEquals(
        IndexSpec.builder().withMetricCompression(CompressionStrategy.NONE).build(),
        config.getIndexSpec()
    );
    Assert.assertEquals(
        IndexSpec.builder().withDimensionCompression(CompressionStrategy.UNCOMPRESSED).build(),
        config.getIndexSpecForIntermediatePersists()
    );
    Assert.assertEquals(2, config.getNumPersistThreads());
  }

  @Test
  public void testConvert()
  {
    KafkaSupervisorTuningConfig original = new KafkaSupervisorTuningConfig(
        null,
        1,
        null,
        null,
        2,
        10L,
        new Period("PT3S"),
        4,
        IndexSpec.DEFAULT,
        IndexSpec.DEFAULT,
        true,
        5L,
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
        2
    );
    KafkaIndexTaskTuningConfig copy = original.convertToTaskTuningConfig();

    Assert.assertEquals(original.getAppendableIndexSpec(), copy.getAppendableIndexSpec());
    Assert.assertEquals(1, copy.getMaxRowsInMemory());
    Assert.assertEquals(2, copy.getMaxRowsPerSegment().intValue());
    Assert.assertNotEquals(null, copy.getMaxTotalRows());
    Assert.assertEquals(10L, copy.getMaxTotalRows().longValue());
    Assert.assertEquals(new Period("PT3S"), copy.getIntermediatePersistPeriod());
    Assert.assertNull(copy.getBasePersistDirectory());
    Assert.assertEquals(4, copy.getMaxPendingPersists());
    Assert.assertEquals(IndexSpec.DEFAULT, copy.getIndexSpec());
    Assert.assertEquals(true, copy.isReportParseExceptions());
    Assert.assertEquals(5L, copy.getHandoffConditionTimeout());
    Assert.assertEquals(2, copy.getNumPersistThreads());
  }

  @Test
  public void testSerdeWithModifiedTuningConfigAddedField() throws IOException
  {
    KafkaIndexTaskTuningConfig base = new KafkaIndexTaskTuningConfig(
        null,
        1,
        null,
        null,
        2,
        10L,
        new Period("PT3S"),
        new File("/tmp/xxx"),
        4,
        IndexSpec.DEFAULT,
        IndexSpec.DEFAULT,
        true,
        5L,
        null,
        null,
        null,
        true,
        42,
        42,
        2
    );

    String serialized = mapper.writeValueAsString(base);
    TestModifiedKafkaIndexTaskTuningConfig deserialized =
        mapper.readValue(serialized, TestModifiedKafkaIndexTaskTuningConfig.class);

    Assert.assertEquals(null, deserialized.getExtra());
    Assert.assertEquals(base.getAppendableIndexSpec(), deserialized.getAppendableIndexSpec());
    Assert.assertEquals(base.getMaxRowsInMemory(), deserialized.getMaxRowsInMemory());
    Assert.assertEquals(base.getMaxBytesInMemory(), deserialized.getMaxBytesInMemory());
    Assert.assertEquals(base.getMaxRowsPerSegment(), deserialized.getMaxRowsPerSegment());
    Assert.assertEquals(base.getMaxTotalRows(), deserialized.getMaxTotalRows());
    Assert.assertEquals(base.getIntermediatePersistPeriod(), deserialized.getIntermediatePersistPeriod());
    Assert.assertNull(deserialized.getBasePersistDirectory());
    Assert.assertEquals(base.getMaxPendingPersists(), deserialized.getMaxPendingPersists());
    Assert.assertEquals(base.getIndexSpec(), deserialized.getIndexSpec());
    Assert.assertEquals(base.isReportParseExceptions(), deserialized.isReportParseExceptions());
    Assert.assertEquals(base.getHandoffConditionTimeout(), deserialized.getHandoffConditionTimeout());
    Assert.assertEquals(base.isResetOffsetAutomatically(), deserialized.isResetOffsetAutomatically());
    Assert.assertEquals(base.getSegmentWriteOutMediumFactory(), deserialized.getSegmentWriteOutMediumFactory());
    Assert.assertEquals(base.getIntermediateHandoffPeriod(), deserialized.getIntermediateHandoffPeriod());
    Assert.assertEquals(base.isLogParseExceptions(), deserialized.isLogParseExceptions());
    Assert.assertEquals(base.getMaxParseExceptions(), deserialized.getMaxParseExceptions());
    Assert.assertEquals(base.getMaxSavedParseExceptions(), deserialized.getMaxSavedParseExceptions());
    Assert.assertEquals(base.getNumPersistThreads(), deserialized.getNumPersistThreads());
  }

  @Test
  public void testSerdeWithModifiedTuningConfigRemovedField() throws IOException
  {
    TestModifiedKafkaIndexTaskTuningConfig base = new TestModifiedKafkaIndexTaskTuningConfig(
        null,
        1,
        null,
        null,
        2,
        10L,
        new Period("PT3S"),
        4,
        IndexSpec.DEFAULT,
        IndexSpec.DEFAULT,
        true,
        5L,
        null,
        null,
        null,
        true,
        42,
        42,
        2,
        "extra string"
    );

    String serialized = mapper.writeValueAsString(base);
    KafkaIndexTaskTuningConfig deserialized =
        mapper.readValue(serialized, KafkaIndexTaskTuningConfig.class);

    Assert.assertEquals(base.getAppendableIndexSpec(), deserialized.getAppendableIndexSpec());
    Assert.assertEquals(base.getMaxRowsInMemory(), deserialized.getMaxRowsInMemory());
    Assert.assertEquals(base.getMaxBytesInMemory(), deserialized.getMaxBytesInMemory());
    Assert.assertEquals(base.getMaxRowsPerSegment(), deserialized.getMaxRowsPerSegment());
    Assert.assertEquals(base.getMaxTotalRows(), deserialized.getMaxTotalRows());
    Assert.assertEquals(base.getIntermediatePersistPeriod(), deserialized.getIntermediatePersistPeriod());
    Assert.assertEquals(base.getBasePersistDirectory(), deserialized.getBasePersistDirectory());
    Assert.assertEquals(base.getMaxPendingPersists(), deserialized.getMaxPendingPersists());
    Assert.assertEquals(base.getIndexSpec(), deserialized.getIndexSpec());
    Assert.assertEquals(base.isReportParseExceptions(), deserialized.isReportParseExceptions());
    Assert.assertEquals(base.getHandoffConditionTimeout(), deserialized.getHandoffConditionTimeout());
    Assert.assertEquals(base.isResetOffsetAutomatically(), deserialized.isResetOffsetAutomatically());
    Assert.assertEquals(base.getSegmentWriteOutMediumFactory(), deserialized.getSegmentWriteOutMediumFactory());
    Assert.assertEquals(base.getIntermediateHandoffPeriod(), deserialized.getIntermediateHandoffPeriod());
    Assert.assertEquals(base.isLogParseExceptions(), deserialized.isLogParseExceptions());
    Assert.assertEquals(base.getMaxParseExceptions(), deserialized.getMaxParseExceptions());
    Assert.assertEquals(base.getMaxSavedParseExceptions(), deserialized.getMaxSavedParseExceptions());
    Assert.assertEquals(base.getNumPersistThreads(), deserialized.getNumPersistThreads());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(KafkaIndexTaskTuningConfig.class)
                  .withPrefabValues(
                      IndexSpec.class,
                      IndexSpec.DEFAULT,
                      IndexSpec.builder().withDimensionCompression(CompressionStrategy.ZSTD).build()
                  )
                  .usingGetClass()
                  .verify();
  }
}
