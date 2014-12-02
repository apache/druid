package io.druid.indexing.common.tasklogs;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.druid.storage.hdfs.tasklog.HdfsTaskLogs;
import io.druid.storage.hdfs.tasklog.HdfsTaskLogsConfig;
import io.druid.tasklogs.TaskLogs;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Map;

public class HdfsTaskLogsTest
{
  @Test
  public void testSimple() throws Exception
  {
    final File tmpDir = Files.createTempDir();
    try {
      final File logDir = new File(tmpDir, "logs");
      final File logFile = new File(tmpDir, "log");
      Files.write("blah", logFile, Charsets.UTF_8);
      final TaskLogs taskLogs = new HdfsTaskLogs(new HdfsTaskLogsConfig(logDir.toString()));
      taskLogs.pushTaskLog("foo", logFile);

      final Map<Long, String> expected = ImmutableMap.of(0L, "blah", 1L, "lah", -2L, "ah", -5L, "blah");
      for (Map.Entry<Long, String> entry : expected.entrySet()) {
        final byte[] bytes = ByteStreams.toByteArray(taskLogs.streamTaskLog("foo", entry.getKey()).get().getInput());
        final String string = new String(bytes);
        Assert.assertEquals(String.format("Read with offset %,d", entry.getKey()), string, entry.getValue());
      }
    }
    finally {
      FileUtils.deleteDirectory(tmpDir);
    }
  }
}
