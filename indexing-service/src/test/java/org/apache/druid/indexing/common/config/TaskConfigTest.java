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
