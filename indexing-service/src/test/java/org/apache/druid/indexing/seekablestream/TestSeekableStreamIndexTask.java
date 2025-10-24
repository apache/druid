/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.seekablestream;

import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.segment.indexing.DataSchema;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Test implementation of SeekableStreamIndexTask for use in unit tests.
 */
public class TestSeekableStreamIndexTask extends SeekableStreamIndexTask<String, String, ByteEntity>
{
  private final SeekableStreamIndexTaskRunner<String, String, ByteEntity> streamingTaskRunner;
  private final RecordSupplier<String, String, ByteEntity> recordSupplier;

  public TestSeekableStreamIndexTask(
      String id,
      @Nullable String supervisorId,
      @Nullable TaskResource taskResource,
      DataSchema dataSchema,
      SeekableStreamIndexTaskTuningConfig tuningConfig,
      SeekableStreamIndexTaskIOConfig<String, String> ioConfig,
      @Nullable Map<String, Object> context,
      @Nullable String groupId
  )
  {
    this(id, supervisorId, taskResource, dataSchema, tuningConfig, ioConfig, context, groupId, null, null);
  }

  public TestSeekableStreamIndexTask(
      String id,
      @Nullable String supervisorId,
      @Nullable TaskResource taskResource,
      DataSchema dataSchema,
      SeekableStreamIndexTaskTuningConfig tuningConfig,
      SeekableStreamIndexTaskIOConfig<String, String> ioConfig,
      @Nullable Map<String, Object> context,
      @Nullable String groupId,
      @Nullable SeekableStreamIndexTaskRunner<String, String, ByteEntity> streamingTaskRunner,
      @Nullable RecordSupplier<String, String, ByteEntity> recordSupplier
  )
  {
    super(id, supervisorId, taskResource, dataSchema, tuningConfig, ioConfig, context, groupId);
    this.streamingTaskRunner = streamingTaskRunner;
    this.recordSupplier = recordSupplier;
  }

  @Nullable
  @Override
  protected SeekableStreamIndexTaskRunner<String, String, ByteEntity> createTaskRunner()
  {
    return streamingTaskRunner;
  }

  @Override
  protected RecordSupplier<String, String, ByteEntity> newTaskRecordSupplier(final TaskToolbox toolbox)
  {
    return recordSupplier;
  }

  @Override
  public String getType()
  {
    return "test_seekable_stream";
  }
}

