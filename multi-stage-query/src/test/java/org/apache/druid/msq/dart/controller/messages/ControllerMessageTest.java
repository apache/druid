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

package org.apache.druid.msq.dart.controller.messages;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.statistics.PartialKeyStatisticsInformation;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

public class ControllerMessageTest
{
  private static final StageId STAGE_ID = StageId.fromString("xyz_2");
  private ObjectMapper objectMapper;

  @BeforeEach
  public void setUp()
  {
    objectMapper = TestHelper.JSON_MAPPER.copy();
    objectMapper.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    objectMapper.registerModules(new MSQIndexingModule().getJacksonModules());
  }

  @Test
  public void testSerde() throws IOException
  {
    final PartialKeyStatisticsInformation partialKeyStatisticsInformation =
        new PartialKeyStatisticsInformation(Collections.emptySet(), false, 0);

    assertSerde(new PartialKeyStatistics(STAGE_ID, 1, partialKeyStatisticsInformation));
    assertSerde(new DoneReadingInput(STAGE_ID, 1));
    assertSerde(new ResultsComplete(STAGE_ID, 1, "foo"));
    assertSerde(
        new WorkerError(
            STAGE_ID.getQueryId(),
            MSQErrorReport.fromFault("task", null, null, UnknownFault.forMessage("oops"))
        )
    );
    assertSerde(
        new WorkerWarning(
            STAGE_ID.getQueryId(),
            Collections.singletonList(MSQErrorReport.fromFault("task", null, null, UnknownFault.forMessage("oops")))
        )
    );
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(PartialKeyStatistics.class).usingGetClass().verify();
    EqualsVerifier.forClass(DoneReadingInput.class).usingGetClass().verify();
    EqualsVerifier.forClass(ResultsComplete.class).usingGetClass().verify();
    EqualsVerifier.forClass(WorkerError.class).usingGetClass().verify();
    EqualsVerifier.forClass(WorkerWarning.class).usingGetClass().verify();
  }

  private void assertSerde(final ControllerMessage message) throws IOException
  {
    final String json = objectMapper.writeValueAsString(message);
    final ControllerMessage message2 = objectMapper.readValue(json, ControllerMessage.class);
    Assertions.assertEquals(message, message2, json);
  }
}
