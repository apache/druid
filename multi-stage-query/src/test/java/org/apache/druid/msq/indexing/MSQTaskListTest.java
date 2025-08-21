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

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MSQTaskListTest
{
  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.simple()
            .forClass(MSQTaskList.class)
            .usingGetClass()
            .withNonnullFields("taskIds")
            .verify();
  }

  @Test
  public void testJsonSerialization() throws Exception
  {
    List<String> taskIds = Arrays.asList("task1", "task2");
    MSQTaskList msqTaskList = new MSQTaskList(taskIds);
    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(msqTaskList);
    MSQTaskList deserialized = mapper.readValue(json, MSQTaskList.class);
    assertEquals(msqTaskList, deserialized);
  }
}
