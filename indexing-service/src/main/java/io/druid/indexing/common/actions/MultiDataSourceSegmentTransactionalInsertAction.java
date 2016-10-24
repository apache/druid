/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.DataSourceMetadataAndSegments;
import io.druid.indexing.overlord.SegmentPublishResult;
import io.druid.query.DruidMetrics;
import io.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.List;

/**
 * SegmentTransactionalInsertAction with multiple dataSource support.
 */
public class MultiDataSourceSegmentTransactionalInsertAction implements TaskAction<SegmentPublishResult>
{
  private final List<DataSourceMetadataAndSegments> metadataAndSegments;

  @JsonCreator
  public MultiDataSourceSegmentTransactionalInsertAction(
      @JsonProperty("metadataAndSegments") List<DataSourceMetadataAndSegments> metadataAndSegments
  )
  {
    this.metadataAndSegments = metadataAndSegments;
  }

  @JsonProperty
  public List<DataSourceMetadataAndSegments> getMetadataAndSegments()
  {
    return metadataAndSegments;
  }

  @Override
  public TypeReference<SegmentPublishResult> getReturnTypeReference()
  {
    return new TypeReference<SegmentPublishResult>()
    {
    };
  }

  @Override
  public SegmentPublishResult perform(Task task, TaskActionToolbox toolbox) throws IOException
  {
    //verify the locks
    for (DataSourceMetadataAndSegments ds : metadataAndSegments) {
      toolbox.verifyTaskLocks(task, ds.getSegments());
    }

    final SegmentPublishResult retVal = toolbox.getIndexerMetadataStorageCoordinator().announceHistoricalSegments(
        metadataAndSegments
    );

    // Emit metrics
    final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder()
        .setDimension(DruidMetrics.DATASOURCE, task.getDataSource())
        .setDimension(DruidMetrics.TASK_TYPE, task.getType());

    if (retVal.isSuccess()) {
      toolbox.getEmitter().emit(metricBuilder.build("segment/txn/success", 1));
    } else {
      toolbox.getEmitter().emit(metricBuilder.build("segment/txn/failure", 1));
    }

    for (DataSegment segment : retVal.getSegments()) {
      metricBuilder.setDimension(DruidMetrics.INTERVAL, segment.getInterval().toString());
      toolbox.getEmitter().emit(metricBuilder.build("segment/added/bytes", segment.getSize()));
    }

    return retVal;
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MultiDataSourceSegmentTransactionalInsertAction that = (MultiDataSourceSegmentTransactionalInsertAction) o;

    return metadataAndSegments.equals(that.metadataAndSegments);

  }

  @Override
  public int hashCode()
  {
    return metadataAndSegments.hashCode();
  }

  @Override
  public String toString()
  {
    return "MultiDataSourceSegmentTransactionalInsertAction{" +
           "metadataAndSegments=" + metadataAndSegments +
           '}';
  }
}
