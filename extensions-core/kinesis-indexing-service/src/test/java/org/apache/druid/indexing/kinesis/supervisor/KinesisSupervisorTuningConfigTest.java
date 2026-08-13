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

package org.apache.druid.indexing.kinesis.supervisor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.kinesis.KinesisIndexingServiceModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.indexing.TuningConfig;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KinesisSupervisorTuningConfigTest
{
  private final ObjectMapper mapper;

  public KinesisSupervisorTuningConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules(new KinesisIndexingServiceModule().getJacksonModules());
  }

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String jsonStr = "{\"type\": \"kinesis\"}";

    KinesisSupervisorTuningConfig config = (KinesisSupervisorTuningConfig) mapper.readValue(
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
    Assertions.assertEquals(false, config.isReportParseExceptions());
    Assertions.assertEquals(java.time.Duration.ofMinutes(15).toMillis(), config.getHandoffConditionTimeout());
    Assertions.assertNull(config.getWorkerThreads());
    Assertions.assertEquals(8L, (long) config.getChatRetries());
    Assertions.assertEquals(Duration.standardSeconds(10), config.getHttpTimeout());
    Assertions.assertEquals(Duration.standardSeconds(80), config.getShutdownTimeout());
    Assertions.assertEquals(Duration.standardSeconds(120), config.getRepartitionTransitionDuration());
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
                     + "  \"workerThreads\": 12,\n"
                     + "  \"chatRetries\": 14,\n"
                     + "  \"httpTimeout\": \"PT15S\",\n"
                     + "  \"shutdownTimeout\": \"PT95S\",\n"
                     + "  \"repartitionTransitionDuration\": \"PT500S\",\n"
                     + "  \"appendableIndexSpec\": { \"type\" : \"onheap\" }\n"
                     + "}";

    KinesisSupervisorTuningConfig config = (KinesisSupervisorTuningConfig) mapper.readValue(
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
    Assertions.assertEquals(true, config.isReportParseExceptions());
    Assertions.assertEquals(100, config.getHandoffConditionTimeout());
    Assertions.assertEquals(12, (int) config.getWorkerThreads());
    Assertions.assertEquals(14L, (long) config.getChatRetries());
    Assertions.assertEquals(Duration.standardSeconds(15), config.getHttpTimeout());
    Assertions.assertEquals(Duration.standardSeconds(95), config.getShutdownTimeout());
    Assertions.assertEquals(Duration.standardSeconds(500), config.getRepartitionTransitionDuration());
  }
}
