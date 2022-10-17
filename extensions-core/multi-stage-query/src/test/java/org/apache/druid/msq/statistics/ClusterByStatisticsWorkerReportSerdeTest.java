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

package org.apache.druid.msq.statistics;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.TreeMap;

public class ClusterByStatisticsWorkerReportSerdeTest
{
  private ObjectMapper objectMapper;

  @Before
  public void setUp()
  {
    objectMapper = TestHelper.makeJsonMapper();
    objectMapper.registerModules(new MSQIndexingModule().getJacksonModules());
    objectMapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    ClusterByStatisticsWorkerReport workerReport = new ClusterByStatisticsWorkerReport(
        new TreeMap<>(ImmutableMap.of(2L, ImmutableSet.of(2, 3))),
        false,
        0.0
    );

    final String json = objectMapper.writeValueAsString(workerReport);
    final ClusterByStatisticsWorkerReport deserializedWorkerReport = objectMapper.readValue(
        json,
        ClusterByStatisticsWorkerReport.class
    );
    Assert.assertEquals(json, workerReport.getTimeSegmentVsWorkerIdMap(), deserializedWorkerReport.getTimeSegmentVsWorkerIdMap());
    Assert.assertEquals(json, workerReport.isHasMultipleValues(), deserializedWorkerReport.isHasMultipleValues());
    Assert.assertEquals(json, workerReport.getBytesRetained(), deserializedWorkerReport.getBytesRetained(), 0);
  }
}
