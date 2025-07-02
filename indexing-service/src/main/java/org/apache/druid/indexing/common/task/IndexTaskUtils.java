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

import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.actions.TaskActionToolbox;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.service.SegmentMetadataEvent;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.timeline.DataSegment;

import java.util.Map;

public class IndexTaskUtils
{
  public static void setTaskDimensions(final ServiceMetricEvent.Builder metricBuilder, final Task task)
  {
    metricBuilder.setDimension(DruidMetrics.TASK_ID, task.getId());
    metricBuilder.setDimension(DruidMetrics.TASK_TYPE, task.getType());
    metricBuilder.setDimensionIfAbsent(DruidMetrics.DATASOURCE, task.getDataSource());
    metricBuilder.setDimensionIfNotNull(
        DruidMetrics.TAGS,
        task.<Map<String, Object>>getContextValue(DruidMetrics.TAGS)
    );
    metricBuilder.setDimensionIfNotNull(DruidMetrics.GROUP_ID, task.getGroupId());
  }

  public static void setTaskDimensions(final ServiceMetricEvent.Builder metricBuilder, final AbstractTask task)
  {
    metricBuilder.setDimension(DruidMetrics.TASK_ID, task.getId());
    metricBuilder.setDimension(DruidMetrics.TASK_TYPE, task.getType());
    metricBuilder.setDimension(DruidMetrics.DATASOURCE, task.getDataSource());
    metricBuilder.setDimension(DruidMetrics.TASK_INGESTION_MODE, task.getIngestionMode());
    metricBuilder.setDimensionIfNotNull(
        DruidMetrics.TAGS,
        task.<Map<String, Object>>getContextValue(DruidMetrics.TAGS)
    );
    metricBuilder.setDimensionIfNotNull(DruidMetrics.GROUP_ID, task.getGroupId());
  }

  public static void setTaskStatusDimensions(
      final ServiceMetricEvent.Builder metricBuilder,
      final TaskStatus taskStatus
  )
  {
    metricBuilder.setDimension(DruidMetrics.TASK_ID, taskStatus.getId());
    metricBuilder.setDimension(DruidMetrics.TASK_STATUS, taskStatus.getStatusCode().toString());

    final String errorMsg = taskStatus.getErrorMsg();
    if (errorMsg != null && !errorMsg.isEmpty()) {
      final String statusDescription = errorMsg.length() > 100 ? errorMsg.substring(0, 100) : errorMsg;
      metricBuilder.setDimension(DruidMetrics.DESCRIPTION, statusDescription);
    }
  }

  public static void setSegmentDimensions(
      ServiceMetricEvent.Builder metricBuilder,
      DataSegment segment
  )
  {
    final String partitionType = segment.getShardSpec() == null ? null : segment.getShardSpec().getType();
    metricBuilder.setDimension(DruidMetrics.PARTITIONING_TYPE, partitionType);
    metricBuilder.setDimension(DruidMetrics.INTERVAL, segment.getInterval().toString());
  }

  public static void emitSegmentPublishMetrics(
      SegmentPublishResult publishResult,
      Task task,
      TaskActionToolbox toolbox
  )
  {
    final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
    IndexTaskUtils.setTaskDimensions(metricBuilder, task);

    if (publishResult.isSuccess()) {
      toolbox.getEmitter().emit(metricBuilder.setMetric("segment/txn/success", 1));
      for (DataSegment segment : publishResult.getSegments()) {
        IndexTaskUtils.setSegmentDimensions(metricBuilder, segment);
        toolbox.getEmitter().emit(metricBuilder.setMetric("segment/added/bytes", segment.getSize()));
        toolbox.getEmitter().emit(SegmentMetadataEvent.create(segment, DateTimes.nowUtc()));
      }
    } else {
      toolbox.getEmitter().emit(metricBuilder.setMetric("segment/txn/failure", 1));
    }
  }
}
