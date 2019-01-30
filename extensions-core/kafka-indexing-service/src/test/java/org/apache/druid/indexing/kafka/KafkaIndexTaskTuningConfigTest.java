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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorTuningConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.TuningConfig;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class KafkaIndexTaskTuningConfigTest
{
  private final ObjectMapper mapper;

  public KafkaIndexTaskTuningConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules((Iterable<Module>) new KafkaIndexTaskModule().getJacksonModules());
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

    Assert.assertNotNull(config.getBasePersistDirectory());
    Assert.assertEquals(1000000, config.getMaxRowsInMemory());
    Assert.assertEquals(5_000_000, config.getMaxRowsPerSegment().intValue());
    Assert.assertEquals(null, config.getMaxTotalRows());
    Assert.assertEquals(new Period("PT10M"), config.getIntermediatePersistPeriod());
    Assert.assertEquals(0, config.getMaxPendingPersists());
    Assert.assertEquals(new IndexSpec(), config.getIndexSpec());
    Assert.assertEquals(false, config.isReportParseExceptions());
    Assert.assertEquals(0, config.getHandoffConditionTimeout());
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
                     + "  \"handoffConditionTimeout\": 100\n"
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

    Assert.assertEquals(new File("/tmp/xxx"), config.getBasePersistDirectory());
    Assert.assertEquals(100, config.getMaxRowsInMemory());
    Assert.assertEquals(100, config.getMaxRowsPerSegment().intValue());
    Assert.assertNotEquals(null, config.getMaxTotalRows());
    Assert.assertEquals(1000, config.getMaxTotalRows().longValue());
    Assert.assertEquals(new Period("PT1H"), config.getIntermediatePersistPeriod());
    Assert.assertEquals(100, config.getMaxPendingPersists());
    Assert.assertEquals(true, config.isReportParseExceptions());
    Assert.assertEquals(100, config.getHandoffConditionTimeout());
  }

  @Test
  public void testConvert()
  {
    KafkaSupervisorTuningConfig original = new KafkaSupervisorTuningConfig(
        1,
        null,
        2,
        10L,
        new Period("PT3S"),
        new File("/tmp/xxx"),
        4,
        new IndexSpec(),
        true,
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
        null
    );
    KafkaIndexTaskTuningConfig copy = (KafkaIndexTaskTuningConfig) original.convertToTaskTuningConfig();

    Assert.assertEquals(1, copy.getMaxRowsInMemory());
    Assert.assertEquals(2, copy.getMaxRowsPerSegment().intValue());
    Assert.assertNotEquals(null, copy.getMaxTotalRows());
    Assert.assertEquals(10L, copy.getMaxTotalRows().longValue());
    Assert.assertEquals(new Period("PT3S"), copy.getIntermediatePersistPeriod());
    Assert.assertEquals(new File("/tmp/xxx"), copy.getBasePersistDirectory());
    Assert.assertEquals(4, copy.getMaxPendingPersists());
    Assert.assertEquals(new IndexSpec(), copy.getIndexSpec());
    Assert.assertEquals(true, copy.isReportParseExceptions());
    Assert.assertEquals(5L, copy.getHandoffConditionTimeout());
  }

  private static KafkaIndexTaskTuningConfig copy(KafkaIndexTaskTuningConfig config)
  {
    return new KafkaIndexTaskTuningConfig(
        config.getMaxRowsInMemory(),
        config.getMaxBytesInMemory(),
        config.getMaxRowsPerSegment(),
        config.getMaxTotalRows(),
        config.getIntermediatePersistPeriod(),
        config.getBasePersistDirectory(),
        0,
        config.getIndexSpec(),
        true,
        config.isReportParseExceptions(),
        config.getHandoffConditionTimeout(),
        config.isResetOffsetAutomatically(),
        config.getSegmentWriteOutMediumFactory(),
        config.getIntermediateHandoffPeriod(),
        config.isLogParseExceptions(),
        config.getMaxParseExceptions(),
        config.getMaxSavedParseExceptions()
    );
  }
}
