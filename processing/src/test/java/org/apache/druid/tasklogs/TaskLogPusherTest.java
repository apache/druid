package org.apache.druid.tasklogs;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TaskLogPusherTest
{
  /**
   * Test default implemenation of pushTaskStatus in TaskLogPusher interface for code coverage
   *
   * @throws IOException
   */
  @Test
  public void test_pushTaskStatus() throws IOException
  {
    TaskLogPusher taskLogPusher = new TaskLogPusher() {
      @Override
      public void pushTaskLog(String taskid, File logFile) throws IOException
      {
      }
    };
    taskLogPusher.pushTaskStatus("id", new File(""));
  }
}
