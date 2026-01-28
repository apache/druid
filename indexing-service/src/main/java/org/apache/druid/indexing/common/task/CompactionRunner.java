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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.client.indexing.ClientCompactionRunnerInfo;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.joda.time.Interval;

import java.util.Map;

/**
 * Strategy to be used for executing a compaction task.
 * Should be synchronized with {@link ClientCompactionRunnerInfo}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = CompactionRunner.TYPE_PROPERTY)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = NativeCompactionRunner.TYPE, value = NativeCompactionRunner.class)
})
public interface CompactionRunner
{
  String TYPE_PROPERTY = "type";

  /**
   * Creates and runs sub-tasks for the given CompactionTask, one interval at a time.
   */
  TaskStatus runCompactionTasks(
      CompactionTask compactionTask,
      Map<Interval, DataSchema> intervalDataSchemaMap,
      TaskToolbox taskToolbox
  ) throws Exception;

  CurrentSubTaskHolder getCurrentSubTaskHolder();

  /**
   * Checks if the provided compaction config is supported by the runner.
   * The same validation is done at {@link org.apache.druid.msq.indexing.MSQCompactionRunner#validateCompactionTask}
   */
  CompactionConfigValidationResult validateCompactionTask(
      CompactionTask compactionTask,
      Map<Interval, DataSchema> intervalToDataSchemaMap
  );

}
