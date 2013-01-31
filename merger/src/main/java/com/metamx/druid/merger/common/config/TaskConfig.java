package com.metamx.druid.merger.common.config;

import com.metamx.druid.merger.common.task.Task;
import org.skife.config.Config;
import org.skife.config.Default;

import java.io.File;

public abstract class TaskConfig
{
  @Config("druid.merger.taskDir")
  public abstract File getBaseTaskDir();

  @Config("druid.merger.rowFlushBoundary")
  @Default("500000")
  public abstract long getRowFlushBoundary();

  public File getTaskDir(final Task task) {
    return new File(getBaseTaskDir(), task.getId());
  }
}
