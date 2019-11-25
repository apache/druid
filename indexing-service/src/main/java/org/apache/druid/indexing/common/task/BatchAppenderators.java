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

import org.apache.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.BatchAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.SegmentAllocator;

public final class BatchAppenderators
{
  public static Appenderator newAppenderator(
      String taskId,
      AppenderatorsManager appenderatorsManager,
      FireDepartmentMetrics metrics,
      TaskToolbox toolbox,
      DataSchema dataSchema,
      AppenderatorConfig appenderatorConfig,
      boolean storeCompactionState
  )
  {
    return newAppenderator(
        taskId,
        appenderatorsManager,
        metrics,
        toolbox,
        dataSchema,
        appenderatorConfig,
        toolbox.getSegmentPusher(),
        storeCompactionState
    );
  }

  public static Appenderator newAppenderator(
      String taskId,
      AppenderatorsManager appenderatorsManager,
      FireDepartmentMetrics metrics,
      TaskToolbox toolbox,
      DataSchema dataSchema,
      AppenderatorConfig appenderatorConfig,
      DataSegmentPusher segmentPusher,
      boolean storeCompactionState
  )
  {
    return appenderatorsManager.createOfflineAppenderatorForTask(
        taskId,
        dataSchema,
        appenderatorConfig.withBasePersistDirectory(toolbox.getPersistDir()),
        storeCompactionState,
        metrics,
        segmentPusher,
        toolbox.getJsonMapper(),
        toolbox.getIndexIO(),
        toolbox.getIndexMergerV9()
    );
  }

  public static BatchAppenderatorDriver newDriver(
      final Appenderator appenderator,
      final TaskToolbox toolbox,
      final SegmentAllocator segmentAllocator
  )
  {
    return new BatchAppenderatorDriver(
        appenderator,
        segmentAllocator,
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        toolbox.getDataSegmentKiller()
    );
  }

  private BatchAppenderators()
  {
  }
}
