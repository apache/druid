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
import org.hamcrest.CoreMatchers;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

  @Rule
  public final ExpectedException exception = ExpectedException.none();

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

    Assert.assertNull(config.getBasePersistDirectory());
    Assert.assertEquals(new OnheapIncrementalIndex.Spec(), config.getAppendableIndexSpec());
    Assert.assertEquals(150000, config.getMaxRowsInMemory());
    Assert.assertEquals(5_000_000, config.getMaxRowsPerSegment().intValue());
    Assert.assertEquals(new Period("PT10M"), config.getIntermediatePersistPeriod());
    Assert.assertEquals(0, config.getMaxPendingPersists());
    Assert.assertEquals(IndexSpec.DEFAULT, config.getIndexSpec());
    Assert.assertFalse(config.isReportParseExceptions());
    Assert.assertEquals(Duration.ofMinutes(15).toMillis(), config.getHandoffConditionTimeout());
    Assert.assertNull(config.getRecordBufferSizeBytesConfigured());
    Assert.assertEquals(100_000_000, config.getRecordBufferSizeBytesOrDefault(2_000_000_000));
    Assert.assertEquals(100_000_000, config.getRecordBufferSizeBytesOrDefault(1_000_000_000));
    Assert.assertEquals(10_000_000, config.getRecordBufferSizeBytesOrDefault(100_000_000));
    Assert.assertEquals(5000, config.getRecordBufferOfferTimeout());
    Assert.assertEquals(5000, config.getRecordBufferFullWait());
    Assert.assertNull(config.getFetchThreads());
    Assert.assertFalse(config.isSkipSequenceNumberAvailabilityCheck());
    Assert.assertFalse(config.isResetOffsetAutomatically());
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

    Assert.assertNull(config.getBasePersistDirectory());
    Assert.assertEquals(new OnheapIncrementalIndex.Spec(), config.getAppendableIndexSpec());
    Assert.assertEquals(100, config.getMaxRowsInMemory());
    Assert.assertEquals(100, config.getMaxRowsPerSegment().intValue());
    Assert.assertEquals(new Period("PT1H"), config.getIntermediatePersistPeriod());
    Assert.assertEquals(100, config.getMaxPendingPersists());
    Assert.assertTrue(config.isReportParseExceptions());
    Assert.assertEquals(100, config.getHandoffConditionTimeout());
    Assert.assertEquals(1000, (int) config.getRecordBufferSizeBytesConfigured());
    Assert.assertEquals(1000, config.getRecordBufferSizeBytesOrDefault(1_000_000_000));
    Assert.assertEquals(500, config.getRecordBufferOfferTimeout());
    Assert.assertEquals(500, config.getRecordBufferFullWait());
    Assert.assertEquals(2, (int) config.getFetchThreads());
    Assert.assertTrue(config.isSkipSequenceNumberAvailabilityCheck());
    Assert.assertFalse(config.isResetOffsetAutomatically());
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
        IndexSpec.DEFAULT,
        IndexSpec.DEFAULT,
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
        new Period("P3D")
    );

    String serialized = mapper.writeValueAsString(base);
    TestModifiedKinesisIndexTaskTuningConfig deserialized =
        mapper.readValue(serialized, TestModifiedKinesisIndexTaskTuningConfig.class);

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
    Assert.assertEquals(base.getRecordBufferFullWait(), deserialized.getRecordBufferFullWait());
    Assert.assertEquals(base.getRecordBufferOfferTimeout(), deserialized.getRecordBufferOfferTimeout());
    Assert.assertEquals(base.getRecordBufferSizeConfigured(), deserialized.getRecordBufferSizeConfigured());
    Assert.assertEquals(base.getRecordBufferSizeBytesConfigured(), deserialized.getRecordBufferSizeBytesConfigured());
    Assert.assertEquals(base.getMaxRecordsPerPollConfigured(), deserialized.getMaxRecordsPerPollConfigured());
    Assert.assertEquals(base.getMaxBytesPerPollConfigured(), deserialized.getMaxBytesPerPollConfigured());
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
        IndexSpec.DEFAULT,
        IndexSpec.DEFAULT,
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
        new Period("P3D")
    );

    String serialized = mapper.writeValueAsString(new TestModifiedKinesisIndexTaskTuningConfig(base, "loool"));
    KinesisIndexTaskTuningConfig deserialized =
        mapper.readValue(serialized, KinesisIndexTaskTuningConfig.class);

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
    Assert.assertEquals(base.getRecordBufferFullWait(), deserialized.getRecordBufferFullWait());
    Assert.assertEquals(base.getRecordBufferOfferTimeout(), deserialized.getRecordBufferOfferTimeout());
    Assert.assertEquals(base.getRecordBufferSizeBytesConfigured(), deserialized.getRecordBufferSizeBytesConfigured());
    Assert.assertEquals(base.getMaxRecordsPerPollConfigured(), deserialized.getMaxRecordsPerPollConfigured());
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

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(IllegalArgumentException.class));
    exception.expectMessage(CoreMatchers.containsString(
        "resetOffsetAutomatically cannot be used if skipSequenceNumberAvailabilityCheck=true"));
    mapper.readValue(jsonStr, TuningConfig.class);
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
        IndexSpec.DEFAULT,
        IndexSpec.DEFAULT,
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
        null
    );
    KinesisIndexTaskTuningConfig copy = original.convertToTaskTuningConfig();

    Assert.assertEquals(original.getAppendableIndexSpec(), copy.getAppendableIndexSpec());
    Assert.assertEquals(1, copy.getMaxRowsInMemory());
    Assert.assertEquals(3, copy.getMaxBytesInMemory());
    Assert.assertEquals(2, copy.getMaxRowsPerSegment().intValue());
    Assert.assertEquals(100L, (long) copy.getMaxTotalRows());
    Assert.assertEquals(new Period("PT3S"), copy.getIntermediatePersistPeriod());
    Assert.assertNull(copy.getBasePersistDirectory());
    Assert.assertEquals(4, copy.getMaxPendingPersists());
    Assert.assertEquals(IndexSpec.DEFAULT, copy.getIndexSpec());
    Assert.assertTrue(copy.isReportParseExceptions());
    Assert.assertEquals(5L, copy.getHandoffConditionTimeout());
    Assert.assertEquals(1000, (int) copy.getRecordBufferSizeBytesConfigured());
    Assert.assertEquals(500, copy.getRecordBufferOfferTimeout());
    Assert.assertEquals(500, copy.getRecordBufferFullWait());
    Assert.assertEquals(2, (int) copy.getFetchThreads());
    Assert.assertFalse(copy.isSkipSequenceNumberAvailabilityCheck());
    Assert.assertTrue(copy.isResetOffsetAutomatically());
    Assert.assertEquals(10, (int) copy.getMaxRecordsPerPollConfigured());
    Assert.assertEquals(new Period().withDays(Integer.MAX_VALUE), copy.getIntermediateHandoffPeriod());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(KinesisIndexTaskTuningConfig.class)
                  .withPrefabValues(
                      IndexSpec.class,
                      IndexSpec.DEFAULT,
                      IndexSpec.builder().withDimensionCompression(CompressionStrategy.ZSTD).build()
                  )
                  .usingGetClass()
                  .verify();
  }
}
