package com.metamx.druid.merger.coordinator;

// TODO move to common or worker?

import com.google.common.base.Optional;
import com.google.common.io.InputSupplier;

import java.io.InputStream;

public interface TaskLogProvider
{
  public Optional<InputSupplier<InputStream>> getLogs(String taskid, long offset);
}
