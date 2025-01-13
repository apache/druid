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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MSQTaskListTest
{
  @Test
  public void testGetTaskIds()
  {
    List<String> taskIds = Arrays.asList("task1", "task2", "task3");
    MSQTaskList msqTaskList = new MSQTaskList(taskIds);
    assertEquals(taskIds, msqTaskList.getTaskIds());
  }

  @Test
  public void testConstructorWithNullTaskIds()
  {
    Executable executable = () -> new MSQTaskList(null);
    assertThrows(NullPointerException.class, executable);
  }

  @Test
  public void testEquals()
  {
    List<String> taskIds1 = Arrays.asList("task1", "task2");
    List<String> taskIds2 = Arrays.asList("task1", "task2");
    MSQTaskList msqTaskList1 = new MSQTaskList(taskIds1);
    MSQTaskList msqTaskList2 = new MSQTaskList(taskIds2);
    assertEquals(msqTaskList1, msqTaskList2);
    assertTrue(msqTaskList1.equals(msqTaskList1));
    assertFalse(msqTaskList1.equals(null));
  }

  @Test
  public void testNotEquals()
  {
    List<String> taskIds1 = Arrays.asList("task1", "task2");
    List<String> taskIds2 = Arrays.asList("task3", "task4");
    MSQTaskList msqTaskList1 = new MSQTaskList(taskIds1);
    MSQTaskList msqTaskList2 = new MSQTaskList(taskIds2);
    assertNotEquals(msqTaskList1, msqTaskList2);
    assertFalse(msqTaskList1.equals(msqTaskList2));
    assertFalse(msqTaskList1.equals(new Object()));
  }

  @Test
  public void testHashCode()
  {
    List<String> taskIds1 = Arrays.asList("task1", "task2");
    MSQTaskList msqTaskList1 = new MSQTaskList(taskIds1);
    MSQTaskList msqTaskList2 = new MSQTaskList(taskIds1);
    assertEquals(msqTaskList1.hashCode(), msqTaskList2.hashCode());
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
