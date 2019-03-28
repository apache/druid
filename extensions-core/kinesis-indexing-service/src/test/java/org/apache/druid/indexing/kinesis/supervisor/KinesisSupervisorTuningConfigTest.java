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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.kinesis.KinesisIndexingServiceModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.TuningConfig;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class KinesisSupervisorTuningConfigTest
{
  private final ObjectMapper mapper;

  public KinesisSupervisorTuningConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules((Iterable<Module>) new KinesisIndexingServiceModule().getJacksonModules());
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

    Assert.assertNotNull(config.getBasePersistDirectory());
    Assert.assertEquals(1000000, config.getMaxRowsInMemory());
    Assert.assertEquals(5_000_000, config.getMaxRowsPerSegment().intValue());
    Assert.assertEquals(new Period("PT10M"), config.getIntermediatePersistPeriod());
    Assert.assertEquals(0, config.getMaxPendingPersists());
    Assert.assertEquals(new IndexSpec(), config.getIndexSpec());
    Assert.assertEquals(true, config.getBuildV9Directly());
    Assert.assertEquals(false, config.isReportParseExceptions());
    Assert.assertEquals(0, config.getHandoffConditionTimeout());
    Assert.assertNull(config.getWorkerThreads());
    Assert.assertNull(config.getChatThreads());
    Assert.assertEquals(8L, (long) config.getChatRetries());
    Assert.assertEquals(Duration.standardSeconds(10), config.getHttpTimeout());
    Assert.assertEquals(Duration.standardSeconds(80), config.getShutdownTimeout());
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
                     + "  \"buildV9Directly\": false,\n"
                     + "  \"reportParseExceptions\": true,\n"
                     + "  \"handoffConditionTimeout\": 100,\n"
                     + "  \"workerThreads\": 12,\n"
                     + "  \"chatThreads\": 13,\n"
                     + "  \"chatRetries\": 14,\n"
                     + "  \"httpTimeout\": \"PT15S\",\n"
                     + "  \"shutdownTimeout\": \"PT95S\"\n"
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

    Assert.assertEquals(new File("/tmp/xxx"), config.getBasePersistDirectory());
    Assert.assertEquals(100, config.getMaxRowsInMemory());
    Assert.assertEquals(100, config.getMaxRowsPerSegment().intValue());
    Assert.assertEquals(new Period("PT1H"), config.getIntermediatePersistPeriod());
    Assert.assertEquals(100, config.getMaxPendingPersists());
    Assert.assertEquals(true, config.getBuildV9Directly());
    Assert.assertEquals(true, config.isReportParseExceptions());
    Assert.assertEquals(100, config.getHandoffConditionTimeout());
    Assert.assertEquals(12, (int) config.getWorkerThreads());
    Assert.assertEquals(13, (int) config.getChatThreads());
    Assert.assertEquals(14L, (long) config.getChatRetries());
    Assert.assertEquals(Duration.standardSeconds(15), config.getHttpTimeout());
    Assert.assertEquals(Duration.standardSeconds(95), config.getShutdownTimeout());
  }
}
