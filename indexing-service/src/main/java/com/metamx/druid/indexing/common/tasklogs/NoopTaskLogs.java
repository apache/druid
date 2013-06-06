package com.metamx.druid.indexing.common.tasklogs;

import com.google.common.base.Optional;
import com.google.common.io.InputSupplier;
import com.metamx.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class NoopTaskLogs implements TaskLogs
{
  private final Logger log = new Logger(TaskLogs.class);

  @Override
  public Optional<InputSupplier<InputStream>> streamTaskLog(String taskid, long offset) throws IOException
  {
    return Optional.absent();
  }

  @Override
  public void pushTaskLog(String taskid, File logFile) throws IOException
  {
    log.info("Not pushing logs for task: %s", taskid);
  }
}
