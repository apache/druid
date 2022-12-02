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

package org.apache.druid.indexing.common.config;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.ISE;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class TaskConfigTest
{
  @Test
  public void testGetTaskBaseDir()
  {
    TaskConfig taskConfig = new TaskConfig(
        null,
        null,
        null,
        null,
        null,
        true,
        null,
        null,
        null,
        false,
        false,
        null,
        null,
        false,
        ImmutableList.of("A", "B", "C")
    );

    // Test round-robin allocation
    Assert.assertEquals(taskConfig.getTaskBaseDir("task0").getPath(), "A");
    Assert.assertEquals(taskConfig.getTaskBaseDir("task1").getPath(), "B");
    Assert.assertEquals(taskConfig.getTaskBaseDir("task2").getPath(), "C");
    Assert.assertEquals(taskConfig.getTaskBaseDir("task3").getPath(), "A");
    Assert.assertEquals(taskConfig.getTaskBaseDir("task4").getPath(), "B");
    Assert.assertEquals(taskConfig.getTaskBaseDir("task5").getPath(), "C");

    // Test that the result is always the same
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(taskConfig.getTaskBaseDir("task0").getPath(), "A");
    }
  }

  @Test
  public void testAddTask()
  {
    TaskConfig taskConfig = new TaskConfig(
        null,
        null,
        null,
        null,
        null,
        true,
        null,
        null,
        null,
        false,
        false,
        null,
        null,
        false,
        ImmutableList.of("A", "B", "C")
    );

    // Test add after get. task0 -> "A"
    Assert.assertEquals(taskConfig.getTaskBaseDir("task0").getPath(), "A");
    taskConfig.addTask("task0", new File("A"));
    Assert.assertEquals(taskConfig.getTaskBaseDir("task0").getPath(), "A");

    // Assign base path directly
    taskConfig.addTask("task1", new File("C"));
    Assert.assertEquals(taskConfig.getTaskBaseDir("task1").getPath(), "C");
  }

  @Test
  public void testAddTaskThrowsISE()
  {
    TaskConfig taskConfig = new TaskConfig(
        null,
        null,
        null,
        null,
        null,
        true,
        null,
        null,
        null,
        false,
        false,
        null,
        null,
        false,
        ImmutableList.of("A", "B", "C")
    );

    // Test add after get. task0 -> "A"
    Assert.assertEquals(taskConfig.getTaskBaseDir("task0").getPath(), "A");
    Assert.assertThrows(ISE.class, () -> taskConfig.addTask("task0", new File("B")));
  }
}
