package org.apache.druid.tasklogs;

import com.google.common.base.Optional;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class TaskLogStreamerTest
{
  /**
   * Test default implemenation of streamTaskStatus in TaskLogStreamer interface for code coverage
   *
   * @throws IOException
   */
  @Test
  public void test_streamTaskStatus() throws IOException
  {
    TaskLogStreamer taskLogStreamer = new TaskLogStreamer() {
      @Override
      public Optional<InputStream> streamTaskLog(String taskid, long offset) throws IOException
      {
        return Optional.absent();
      }
    };
    Assert.assertFalse(taskLogStreamer.streamTaskStatus("id").isPresent());
  }
}
