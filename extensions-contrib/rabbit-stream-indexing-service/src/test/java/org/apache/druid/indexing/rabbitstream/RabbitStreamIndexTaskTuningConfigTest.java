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

package org.apache.druid.indexing.rabbitstream;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.indexing.TuningConfig;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Duration;

public class RabbitStreamIndexTaskTuningConfigTest
{
  private final ObjectMapper mapper;

  public RabbitStreamIndexTaskTuningConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules((Iterable<Module>) new RabbitStreamIndexTaskModule().getJacksonModules());
  }

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String jsonStr = "{\"type\": \"rabbit\"}";

    RabbitStreamIndexTaskTuningConfig config = (RabbitStreamIndexTaskTuningConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                TuningConfig.class)),
        TuningConfig.class);

    Assert.assertNull(config.getBasePersistDirectory());
    Assert.assertEquals(new OnheapIncrementalIndex.Spec(), config.getAppendableIndexSpec());
    Assert.assertEquals(150000, config.getMaxRowsInMemory());
    Assert.assertEquals(5_000_000, config.getMaxRowsPerSegment().intValue());
    Assert.assertEquals(new Period("PT10M"), config.getIntermediatePersistPeriod());
    Assert.assertEquals(0, config.getMaxPendingPersists());
    // Assert.assertEquals(IndexSpec.DEFAULT, config.getIndexSpec());
    Assert.assertEquals(false, config.isReportParseExceptions());
    Assert.assertEquals(Duration.ofMinutes(15).toMillis(), config.getHandoffConditionTimeout());

    Assert.assertNull(config.getRecordBufferSizeConfigured());
    Assert.assertEquals(10000, config.getRecordBufferSizeOrDefault(1_000_000_000));
    Assert.assertEquals(5000, config.getRecordBufferOfferTimeout());

    Assert.assertFalse(config.isSkipSequenceNumberAvailabilityCheck());
    Assert.assertFalse(config.isResetOffsetAutomatically());
  }

  @Test
  public void testSerdeWithNonDefaults() throws Exception
  {
    String jsonStr = "{\n"
        + "  \"type\": \"rabbit\",\n"
        + "  \"basePersistDirectory\": \"/tmp/xxx\",\n"
        + "  \"maxRowsInMemory\": 100,\n"
        + "  \"maxRowsPerSegment\": 100,\n"
        + "  \"intermediatePersistPeriod\": \"PT1H\",\n"
        + "  \"maxPendingPersists\": 100,\n"
        + "  \"reportParseExceptions\": true,\n"
        + "  \"handoffConditionTimeout\": 100,\n"
        + "  \"recordBufferSize\": 1000,\n"
        + "  \"recordBufferOfferTimeout\": 500,\n"
        + "  \"resetOffsetAutomatically\": false,\n"
        + "  \"appendableIndexSpec\": { \"type\" : \"onheap\" }\n"
        + "}";

    RabbitStreamIndexTaskTuningConfig config = (RabbitStreamIndexTaskTuningConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                TuningConfig.class)),
        TuningConfig.class);

    Assert.assertNull(config.getBasePersistDirectory());
    Assert.assertEquals(new OnheapIncrementalIndex.Spec(), config.getAppendableIndexSpec());
    Assert.assertEquals(100, config.getMaxRowsInMemory());
    Assert.assertEquals(100, config.getMaxRowsPerSegment().intValue());
    Assert.assertEquals(new Period("PT1H"), config.getIntermediatePersistPeriod());
    Assert.assertEquals(100, config.getMaxPendingPersists());
    Assert.assertTrue(config.isReportParseExceptions());
    Assert.assertEquals(100, config.getHandoffConditionTimeout());
    Assert.assertEquals(1000, (int) config.getRecordBufferSizeConfigured());
    Assert.assertEquals(1000, config.getRecordBufferSizeOrDefault(1_000_000_000));
    Assert.assertEquals(500, config.getRecordBufferOfferTimeout());
    Assert.assertFalse(config.isResetOffsetAutomatically());
  }


  @Test
  public void testtoString() throws Exception
  {
    String jsonStr = "{\n"
        + "  \"type\": \"rabbit\",\n"
        + "  \"basePersistDirectory\": \"/tmp/xxx\",\n"
        + "  \"maxRowsInMemory\": 100,\n"
        + "  \"maxRowsPerSegment\": 100,\n"
        + "  \"intermediatePersistPeriod\": \"PT1H\",\n"
        + "  \"maxPendingPersists\": 100,\n"
        + "  \"reportParseExceptions\": true,\n"
        + "  \"handoffConditionTimeout\": 100,\n"
        + "  \"recordBufferSize\": 1000,\n"
        + "  \"recordBufferOfferTimeout\": 500,\n"
        + "  \"resetOffsetAutomatically\": false,\n"
        + "  \"appendableIndexSpec\": { \"type\" : \"onheap\" }\n"
        + "}";

    RabbitStreamIndexTaskTuningConfig config = (RabbitStreamIndexTaskTuningConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                TuningConfig.class)),
        TuningConfig.class);

    String resStr = "RabbitStreamSupervisorTuningConfig{" +
        "maxRowsInMemory=100, " +
        "maxRowsPerSegment=100, " +
        "maxTotalRows=null, " +
        "maxBytesInMemory=" + config.getMaxBytesInMemoryOrDefault() + ", " +
        "skipBytesInMemoryOverheadCheck=false, " +
        "intermediatePersistPeriod=PT1H, " +
        "maxPendingPersists=100, " +
        "indexSpec=IndexSpec{" +
        "bitmapSerdeFactory=RoaringBitmapSerdeFactory{}, " +
        "dimensionCompression=lz4, " +
        "stringDictionaryEncoding=Utf8{}, " +
        "metricCompression=lz4, " +
        "longEncoding=longs, " +
        "jsonCompression=null, " +
        "segmentLoader=null" +
        "}, " +
        "reportParseExceptions=true, " +
        "handoffConditionTimeout=100, " +
        "resetOffsetAutomatically=false, " +
        "segmentWriteOutMediumFactory=null, " +
        "workerThreads=null, " +
        "chatThreads=null, " +
        "chatRetries=8, " +
        "httpTimeout=PT10S, " +
        "shutdownTimeout=PT80S, " +
        "recordBufferSize=1000, " +
        "recordBufferOfferTimeout=500, " +
        "offsetFetchPeriod=PT30S, " +
        "intermediateHandoffPeriod=" + config.getIntermediateHandoffPeriod() + ", " +
        "logParseExceptions=false, " +
        "maxParseExceptions=0, " +
        "maxSavedParseExceptions=0, " +
        "numPersistThreads=1, " +
        "maxRecordsPerPoll=null}";
  

    Assert.assertEquals(resStr, config.toString());
  }
}
