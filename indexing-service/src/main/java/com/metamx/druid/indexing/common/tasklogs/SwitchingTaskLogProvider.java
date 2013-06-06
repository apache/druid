package com.metamx.druid.indexing.common.tasklogs;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.io.InputSupplier;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Provides task logs based on a series of underlying task log providers.
 */
public class SwitchingTaskLogProvider implements TaskLogProvider
{
  private final List<TaskLogProvider> providers;

  public SwitchingTaskLogProvider(List<TaskLogProvider> providers)
  {
    this.providers = ImmutableList.copyOf(providers);
  }

  @Override
  public Optional<InputSupplier<InputStream>> streamTaskLog(String taskid, long offset) throws IOException
  {
    for (TaskLogProvider provider : providers) {
      final Optional<InputSupplier<InputStream>> stream = provider.streamTaskLog(taskid, offset);
      if (stream.isPresent()) {
        return stream;
      }
    }

    return Optional.absent();
  }
}
