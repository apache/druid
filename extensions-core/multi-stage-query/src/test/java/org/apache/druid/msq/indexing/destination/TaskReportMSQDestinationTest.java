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

package org.apache.druid.msq.indexing.destination;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.sql.http.ResultFormat;
import org.junit.Assert;
import org.junit.Test;

public class TaskReportMSQDestinationTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(TaskReportMSQDestination.class)
                  .withNonnullFields("resultFormat")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();

    TaskReportMSQDestination msqDestination = new TaskReportMSQDestination(ResultFormat.OBJECTLINES);

    final TaskReportMSQDestination msqDestination2 = mapper.readValue(
        mapper.writeValueAsString(msqDestination),
        TaskReportMSQDestination.class
    );

    Assert.assertEquals(msqDestination, msqDestination2);
  }

  @Test
  public void testDefaultResultFormat() throws Exception
  {
    final ObjectMapper mapper = TestHelper.makeJsonMapper();
    String destinationString = "{\"type\":\"taskReport\"}";


    final TaskReportMSQDestination msqDestination = mapper.readValue(
        destinationString,
        TaskReportMSQDestination.class
    );

    Assert.assertEquals(ResultFormat.DEFAULT_RESULT_FORMAT, msqDestination.getResultFormat());
  }
}
