/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.segment.IndexSpec;
import io.druid.segment.TestHelper;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class RealtimeTuningConfigTest
{
  @Test
  public void testDefaultBasePersistDirectory()
  {
    final RealtimeTuningConfig tuningConfig1 = RealtimeTuningConfig.makeDefaultTuningConfig(null);
    final RealtimeTuningConfig tuningConfig2 = RealtimeTuningConfig.makeDefaultTuningConfig(null);
    Assert.assertNotEquals(tuningConfig1.getBasePersistDirectory(), tuningConfig2.getBasePersistDirectory());
  }

  @Test
  public void testSpecificBasePersistDirectory()
  {
    final RealtimeTuningConfig tuningConfig = RealtimeTuningConfig.makeDefaultTuningConfig(
        new File("/tmp/nonexistent")
    );
    Assert.assertEquals(new File("/tmp/nonexistent"), tuningConfig.getBasePersistDirectory());
  }

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String jsonStr = "{\"type\":\"realtime\"}";

    ObjectMapper mapper = TestHelper.makeJsonMapper();
    RealtimeTuningConfig config = (RealtimeTuningConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                TuningConfig.class
            )
        ),
        TuningConfig.class
    );

    Assert.assertNotNull(config.getBasePersistDirectory());
    Assert.assertEquals(0, config.getHandoffConditionTimeout());
    Assert.assertEquals(0, config.getAlertTimeout());
    Assert.assertEquals(new IndexSpec(), config.getIndexSpec());
    Assert.assertEquals(new Period("PT10M"), config.getIntermediatePersistPeriod());
    Assert.assertEquals(NoneShardSpec.instance(), config.getShardSpec());
    Assert.assertEquals(0, config.getMaxPendingPersists());
    Assert.assertEquals(75000, config.getMaxRowsInMemory());
    Assert.assertEquals(0, config.getMergeThreadPriority());
    Assert.assertEquals(0, config.getPersistThreadPriority());
    Assert.assertEquals(new Period("PT10M"), config.getWindowPeriod());
    Assert.assertEquals(false, config.isReportParseExceptions());
  }

  @Test
  public void testSerdeWithNonDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"realtime\",\n"
                     + "  \"maxRowsInMemory\": 100,\n"
                     + "  \"intermediatePersistPeriod\": \"PT1H\",\n"
                     + "  \"windowPeriod\": \"PT1H\",\n"
                     + "  \"basePersistDirectory\": \"/tmp/xxx\",\n"
                     + "  \"maxPendingPersists\": 100,\n"
                     + "  \"persistThreadPriority\": 100,\n"
                     + "  \"mergeThreadPriority\": 100,\n"
                     + "  \"reportParseExceptions\": true,\n"
                     + "  \"handoffConditionTimeout\": 100,\n"
                     + "  \"alertTimeout\": 70\n"
                     + "}";

    ObjectMapper mapper = TestHelper.makeJsonMapper();
    RealtimeTuningConfig config = (RealtimeTuningConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                TuningConfig.class
            )
        ),
        TuningConfig.class
    );

    Assert.assertEquals("/tmp/xxx", config.getBasePersistDirectory().toString());
    Assert.assertEquals(100, config.getHandoffConditionTimeout());
    Assert.assertEquals(70, config.getAlertTimeout());
    Assert.assertEquals(new IndexSpec(), config.getIndexSpec());
    Assert.assertEquals(new Period("PT1H"), config.getIntermediatePersistPeriod());
    Assert.assertEquals(NoneShardSpec.instance(), config.getShardSpec());
    Assert.assertEquals(100, config.getMaxPendingPersists());
    Assert.assertEquals(100, config.getMaxRowsInMemory());
    Assert.assertEquals(100, config.getMergeThreadPriority());
    Assert.assertEquals(100, config.getPersistThreadPriority());
    Assert.assertEquals(new Period("PT1H"), config.getWindowPeriod());
    Assert.assertEquals(true, config.isReportParseExceptions());
  }
}
