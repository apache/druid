package com.metamx.druid.indexing.common.tasklogs;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.io.InputSupplier;
import com.google.inject.Inject;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Provides task logs based on a series of underlying task log providers.
 */
public class SwitchingTaskLogStreamer implements TaskLogStreamer
{
  private final List<TaskLogStreamer> providers;

  @Inject
  public SwitchingTaskLogStreamer(List<TaskLogStreamer> providers)
  {
    this.providers = ImmutableList.copyOf(providers);
  }

  @Override
  public Optional<InputSupplier<InputStream>> streamTaskLog(String taskid, long offset) throws IOException
  {
    for (TaskLogStreamer provider : providers) {
      final Optional<InputSupplier<InputStream>> stream = provider.streamTaskLog(taskid, offset);
      if (stream.isPresent()) {
        return stream;
      }
    }

    return Optional.absent();
  }
}
