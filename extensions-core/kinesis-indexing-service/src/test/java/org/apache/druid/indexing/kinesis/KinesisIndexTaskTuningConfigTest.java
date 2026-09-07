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

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.indexing.kinesis.supervisor.KinesisSupervisorTuningConfig;
import org.apache.druid.indexing.kinesis.test.TestModifiedKinesisIndexTaskTuningConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.indexing.TuningConfig;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;

public class KinesisIndexTaskTuningConfigTest
{
  private final ObjectMapper mapper;

  public KinesisIndexTaskTuningConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules(new KinesisIndexingServiceModule().getJacksonModules());
  }

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String jsonStr = "{\"type\": \"kinesis\"}";

    KinesisIndexTaskTuningConfig config = (KinesisIndexTaskTuningConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                TuningConfig.class
            )
        ),
        TuningConfig.class
    );

    Assertions.assertNull(config.getBasePersistDirectory());
    Assertions.assertEquals(new OnheapIncrementalIndex.Spec(), config.getAppendableIndexSpec());
    Assertions.assertEquals(150000, config.getMaxRowsInMemory());
    Assertions.assertEquals(5_000_000, config.getMaxRowsPerSegment().intValue());
    Assertions.assertEquals(new Period("PT10M"), config.getIntermediatePersistPeriod());
    Assertions.assertEquals(0, config.getMaxPendingPersists());
    Assertions.assertEquals(IndexSpec.getDefault(), config.getIndexSpec());
    Assertions.assertFalse(config.isReportParseExceptions());
    Assertions.assertEquals(Duration.ofMinutes(15).toMillis(), config.getHandoffConditionTimeout());
    Assertions.assertNull(config.getRecordBufferSizeBytesConfigured());
    Assertions.assertEquals(100_000_000, config.getRecordBufferSizeBytesOrDefault(2_000_000_000));
    Assertions.assertEquals(100_000_000, config.getRecordBufferSizeBytesOrDefault(1_000_000_000));
    Assertions.assertEquals(10_000_000, config.getRecordBufferSizeBytesOrDefault(100_000_000));
    Assertions.assertEquals(5000, config.getRecordBufferOfferTimeout());
    Assertions.assertEquals(5000, config.getRecordBufferFullWait());
    Assertions.assertNull(config.getFetchThreads());
    Assertions.assertFalse(config.isSkipSequenceNumberAvailabilityCheck());
    Assertions.assertFalse(config.isResetOffsetAutomatically());
  }

  @Test
  public void testSerdeWithNonDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"basePersistDirectory\": \"/tmp/xxx\",\n"
                     + "  \"maxRowsInMemory\": 100,\n"
                     + "  \"maxRowsPerSegment\": 100,\n"
                     + "  \"intermediatePersistPeriod\": \"PT1H\",\n"
                     + "  \"maxPendingPersists\": 100,\n"
                     + "  \"reportParseExceptions\": true,\n"
                     + "  \"handoffConditionTimeout\": 100,\n"
                     + "  \"recordBufferSizeBytes\": 1000,\n"
                     + "  \"recordBufferOfferTimeout\": 500,\n"
                     + "  \"recordBufferFullWait\": 500,\n"
                     + "  \"resetOffsetAutomatically\": false,\n"
                     + "  \"skipSequenceNumberAvailabilityCheck\": true,\n"
                     + "  \"fetchThreads\": 2,\n"
                     + "  \"appendableIndexSpec\": { \"type\" : \"onheap\" }\n"
                     + "}";

    KinesisIndexTaskTuningConfig config = (KinesisIndexTaskTuningConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                TuningConfig.class
            )
        ),
        TuningConfig.class
    );

    Assertions.assertNull(config.getBasePersistDirectory());
    Assertions.assertEquals(new OnheapIncrementalIndex.Spec(), config.getAppendableIndexSpec());
    Assertions.assertEquals(100, config.getMaxRowsInMemory());
    Assertions.assertEquals(100, config.getMaxRowsPerSegment().intValue());
    Assertions.assertEquals(new Period("PT1H"), config.getIntermediatePersistPeriod());
    Assertions.assertEquals(100, config.getMaxPendingPersists());
    Assertions.assertTrue(config.isReportParseExceptions());
    Assertions.assertEquals(100, config.getHandoffConditionTimeout());
    Assertions.assertEquals(1000, (int) config.getRecordBufferSizeBytesConfigured());
    Assertions.assertEquals(1000, config.getRecordBufferSizeBytesOrDefault(1_000_000_000));
    Assertions.assertEquals(500, config.getRecordBufferOfferTimeout());
    Assertions.assertEquals(500, config.getRecordBufferFullWait());
    Assertions.assertEquals(2, (int) config.getFetchThreads());
    Assertions.assertTrue(config.isSkipSequenceNumberAvailabilityCheck());
    Assertions.assertFalse(config.isResetOffsetAutomatically());
    Assertions.assertEquals(-1, config.getMaxColumnsToMerge());

  }

  @Test
  public void testSerdeWithModifiedTuningConfigAddedField() throws IOException
  {
    KinesisIndexTaskTuningConfig base = new KinesisIndexTaskTuningConfig(
        null,
        1,
        3L,
        null,
        2,
        100L,
        new Period("PT3S"),
        new File("/tmp/xxx"),
        4,
        IndexSpec.getDefault(),
        IndexSpec.getDefault(),
        true,
        5L,
        true,
        false,
        null,
        1000,
        1000,
        500,
        42,
        null,
        false,
        500,
        500,
        6000,
        1_000_000,
        new Period("P3D"),
        1000
    );

    String serialized = mapper.writeValueAsString(base);
    TestModifiedKinesisIndexTaskTuningConfig deserialized =
        mapper.readValue(serialized, TestModifiedKinesisIndexTaskTuningConfig.class);

    Assertions.assertEquals(null, deserialized.getExtra());
    Assertions.assertEquals(base.getAppendableIndexSpec(), deserialized.getAppendableIndexSpec());
    Assertions.assertEquals(base.getMaxRowsInMemory(), deserialized.getMaxRowsInMemory());
    Assertions.assertEquals(base.getMaxBytesInMemory(), deserialized.getMaxBytesInMemory());
    Assertions.assertEquals(base.getMaxRowsPerSegment(), deserialized.getMaxRowsPerSegment());
    Assertions.assertEquals(base.getMaxTotalRows(), deserialized.getMaxTotalRows());
    Assertions.assertEquals(base.getIntermediatePersistPeriod(), deserialized.getIntermediatePersistPeriod());
    Assertions.assertNull(deserialized.getBasePersistDirectory());
    Assertions.assertEquals(base.getMaxPendingPersists(), deserialized.getMaxPendingPersists());
    Assertions.assertEquals(base.getIndexSpec(), deserialized.getIndexSpec());
    Assertions.assertEquals(base.isReportParseExceptions(), deserialized.isReportParseExceptions());
    Assertions.assertEquals(base.getHandoffConditionTimeout(), deserialized.getHandoffConditionTimeout());
    Assertions.assertEquals(base.isResetOffsetAutomatically(), deserialized.isResetOffsetAutomatically());
    Assertions.assertEquals(base.getSegmentWriteOutMediumFactory(), deserialized.getSegmentWriteOutMediumFactory());
    Assertions.assertEquals(base.getIntermediateHandoffPeriod(), deserialized.getIntermediateHandoffPeriod());
    Assertions.assertEquals(base.isLogParseExceptions(), deserialized.isLogParseExceptions());
    Assertions.assertEquals(base.getMaxParseExceptions(), deserialized.getMaxParseExceptions());
    Assertions.assertEquals(base.getMaxSavedParseExceptions(), deserialized.getMaxSavedParseExceptions());
    Assertions.assertEquals(base.getRecordBufferFullWait(), deserialized.getRecordBufferFullWait());
    Assertions.assertEquals(base.getRecordBufferOfferTimeout(), deserialized.getRecordBufferOfferTimeout());
    Assertions.assertEquals(base.getRecordBufferSizeConfigured(), deserialized.getRecordBufferSizeConfigured());
    Assertions.assertEquals(base.getRecordBufferSizeBytesConfigured(), deserialized.getRecordBufferSizeBytesConfigured());
    Assertions.assertEquals(base.getMaxRecordsPerPollConfigured(), deserialized.getMaxRecordsPerPollConfigured());
    Assertions.assertEquals(base.getMaxBytesPerPollConfigured(), deserialized.getMaxBytesPerPollConfigured());
    Assertions.assertEquals(base.getMaxColumnsToMerge(), deserialized.getMaxColumnsToMerge());
  }

  @Test
  public void testSerdeWithModifiedTuningConfigRemovedField() throws IOException
  {
    KinesisIndexTaskTuningConfig base = new KinesisIndexTaskTuningConfig(
        null,
        1,
        3L,
        null,
        2,
        100L,
        new Period("PT3S"),
        new File("/tmp/xxx"),
        4,
        IndexSpec.getDefault(),
        IndexSpec.getDefault(),
        true,
        5L,
        true,
        false,
        null,
        1000,
        1000,
        500,
        42,
        null,
        false,
        500,
        500,
        1_000_000,
        6000,
        new Period("P3D"),
        1000
    );

    String serialized = mapper.writeValueAsString(new TestModifiedKinesisIndexTaskTuningConfig(base, "loool"));
    KinesisIndexTaskTuningConfig deserialized =
        mapper.readValue(serialized, KinesisIndexTaskTuningConfig.class);

    Assertions.assertEquals(base.getAppendableIndexSpec(), deserialized.getAppendableIndexSpec());
    Assertions.assertEquals(base.getMaxRowsInMemory(), deserialized.getMaxRowsInMemory());
    Assertions.assertEquals(base.getMaxBytesInMemory(), deserialized.getMaxBytesInMemory());
    Assertions.assertEquals(base.getMaxRowsPerSegment(), deserialized.getMaxRowsPerSegment());
    Assertions.assertEquals(base.getMaxTotalRows(), deserialized.getMaxTotalRows());
    Assertions.assertEquals(base.getIntermediatePersistPeriod(), deserialized.getIntermediatePersistPeriod());
    Assertions.assertNull(deserialized.getBasePersistDirectory());
    Assertions.assertEquals(base.getMaxPendingPersists(), deserialized.getMaxPendingPersists());
    Assertions.assertEquals(base.getIndexSpec(), deserialized.getIndexSpec());
    Assertions.assertEquals(base.isReportParseExceptions(), deserialized.isReportParseExceptions());
    Assertions.assertEquals(base.getHandoffConditionTimeout(), deserialized.getHandoffConditionTimeout());
    Assertions.assertEquals(base.isResetOffsetAutomatically(), deserialized.isResetOffsetAutomatically());
    Assertions.assertEquals(base.getSegmentWriteOutMediumFactory(), deserialized.getSegmentWriteOutMediumFactory());
    Assertions.assertEquals(base.getIntermediateHandoffPeriod(), deserialized.getIntermediateHandoffPeriod());
    Assertions.assertEquals(base.isLogParseExceptions(), deserialized.isLogParseExceptions());
    Assertions.assertEquals(base.getMaxParseExceptions(), deserialized.getMaxParseExceptions());
    Assertions.assertEquals(base.getMaxSavedParseExceptions(), deserialized.getMaxSavedParseExceptions());
    Assertions.assertEquals(base.getRecordBufferFullWait(), deserialized.getRecordBufferFullWait());
    Assertions.assertEquals(base.getRecordBufferOfferTimeout(), deserialized.getRecordBufferOfferTimeout());
    Assertions.assertEquals(base.getRecordBufferSizeBytesConfigured(), deserialized.getRecordBufferSizeBytesConfigured());
    Assertions.assertEquals(base.getMaxRecordsPerPollConfigured(), deserialized.getMaxRecordsPerPollConfigured());
    Assertions.assertEquals(base.getMaxColumnsToMerge(), deserialized.getMaxColumnsToMerge());
  }

  @Test
  public void testResetOffsetAndSkipSequenceNotBothTrue() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kinesis\",\n"
                     + "  \"basePersistDirectory\": \"/tmp/xxx\",\n"
                     + "  \"maxRowsInMemory\": 100,\n"
                     + "  \"maxRowsPerSegment\": 100,\n"
                     + "  \"intermediatePersistPeriod\": \"PT1H\",\n"
                     + "  \"maxPendingPersists\": 100,\n"
                     + "  \"reportParseExceptions\": true,\n"
                     + "  \"handoffConditionTimeout\": 100,\n"
                     + "  \"recordBufferSize\": 1000,\n"
                     + "  \"recordBufferOfferTimeout\": 500,\n"
                     + "  \"recordBufferFullWait\": 500,\n"
                     + "  \"resetOffsetAutomatically\": true,\n"
                     + "  \"skipSequenceNumberAvailabilityCheck\": true,\n"
                     + "  \"fetchThreads\": 2\n"
                     + "}";

    JsonMappingException exception = Assertions.assertThrows(
        JsonMappingException.class,
        () -> mapper.readValue(jsonStr, TuningConfig.class)
    );
    Assertions.assertInstanceOf(IllegalArgumentException.class, exception.getCause());
    Assertions.assertTrue(
        exception.getMessage().contains(
            "resetOffsetAutomatically cannot be used if skipSequenceNumberAvailabilityCheck=true"
        )
    );
  }

  @Test
  public void testConvert()
  {
    KinesisSupervisorTuningConfig original = new KinesisSupervisorTuningConfig(
        null,
        1,
        (long) 3,
        null,
        2,
        100L,
        new Period("PT3S"),
        4,
        IndexSpec.getDefault(),
        IndexSpec.getDefault(),
        true,
        5L,
        true,
        false,
        null,
        null,
        null,
        null,
        null,
        null,
        1000,
        500,
        500,
        2,
        null,
        null,
        null,
        10,
        1_000_000,
        null,
        null,
        null,
        null,
        null
    );
    KinesisIndexTaskTuningConfig copy = original.convertToTaskTuningConfig();

    Assertions.assertEquals(original.getAppendableIndexSpec(), copy.getAppendableIndexSpec());
    Assertions.assertEquals(1, copy.getMaxRowsInMemory());
    Assertions.assertEquals(3, copy.getMaxBytesInMemory());
    Assertions.assertEquals(2, copy.getMaxRowsPerSegment().intValue());
    Assertions.assertEquals(100L, (long) copy.getMaxTotalRows());
    Assertions.assertEquals(new Period("PT3S"), copy.getIntermediatePersistPeriod());
    Assertions.assertNull(copy.getBasePersistDirectory());
    Assertions.assertEquals(4, copy.getMaxPendingPersists());
    Assertions.assertEquals(IndexSpec.getDefault(), copy.getIndexSpec());
    Assertions.assertTrue(copy.isReportParseExceptions());
    Assertions.assertEquals(5L, copy.getHandoffConditionTimeout());
    Assertions.assertEquals(1000, (int) copy.getRecordBufferSizeBytesConfigured());
    Assertions.assertEquals(500, copy.getRecordBufferOfferTimeout());
    Assertions.assertEquals(500, copy.getRecordBufferFullWait());
    Assertions.assertEquals(2, (int) copy.getFetchThreads());
    Assertions.assertFalse(copy.isSkipSequenceNumberAvailabilityCheck());
    Assertions.assertTrue(copy.isResetOffsetAutomatically());
    Assertions.assertEquals(10, (int) copy.getMaxRecordsPerPollConfigured());
    Assertions.assertEquals(new Period().withDays(Integer.MAX_VALUE), copy.getIntermediateHandoffPeriod());
    Assertions.assertEquals(-1, copy.getMaxColumnsToMerge());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(KinesisIndexTaskTuningConfig.class)
                  .withPrefabValues(
                      IndexSpec.class,
                      IndexSpec.getDefault(),
                      IndexSpec.builder().withDimensionCompression(CompressionStrategy.ZSTD).build()
                  )
                  .usingGetClass()
                  .verify();
  }
}
