/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.common.tasklogs;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.io.InputSupplier;
import com.google.inject.Inject;
import io.druid.tasklogs.TaskLogStreamer;

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
  public Optional<ByteSource> streamTaskLog(String taskid, long offset) throws IOException
  {
    for (TaskLogStreamer provider : providers) {
      final Optional<ByteSource> stream = provider.streamTaskLog(taskid, offset);
      if (stream.isPresent()) {
        return stream;
      }
    }

    return Optional.absent();
  }
}
