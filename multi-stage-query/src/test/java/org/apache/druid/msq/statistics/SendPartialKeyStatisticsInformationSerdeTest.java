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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SendPartialKeyStatisticsInformationSerdeTest
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
    PartialKeyStatisticsInformation partialInformation = new PartialKeyStatisticsInformation(
        ImmutableSet.of(2L, 3L),
        false,
        0.0
    );

    final String json = objectMapper.writeValueAsString(partialInformation);
    final PartialKeyStatisticsInformation deserializedKeyStatistics = objectMapper.readValue(
        json,
        PartialKeyStatisticsInformation.class
    );
    Assert.assertEquals(json, partialInformation.getTimeSegments(), deserializedKeyStatistics.getTimeSegments());
    Assert.assertEquals(json, partialInformation.hasMultipleValues(), deserializedKeyStatistics.hasMultipleValues());
    Assert.assertEquals(json, partialInformation.getBytesRetained(), deserializedKeyStatistics.getBytesRetained(), 0);
  }
}
