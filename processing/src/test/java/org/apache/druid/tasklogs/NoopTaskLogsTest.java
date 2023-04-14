package org.apache.druid.tasklogs;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class NoopTaskLogsTest
{
  @Test
  public void test_streamTaskStatus() throws IOException
  {
    TaskLogs taskLogs =  new NoopTaskLogs();
    Assert.assertFalse(taskLogs.streamTaskStatus("id").isPresent());
  }
}
