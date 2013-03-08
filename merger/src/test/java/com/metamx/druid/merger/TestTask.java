/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.merger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.task.MergeTask;

import java.util.List;

/**
 */
@JsonTypeName("test")
public class TestTask extends MergeTask
{
  private final String id;
  private final TaskStatus status;

  @JsonCreator
  public TestTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("segments") List<DataSegment> segments,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregators,
      @JsonProperty("taskStatus") TaskStatus status
  )
  {
    super(dataSource, segments, aggregators);

    this.id = id;
    this.status = status;
  }

  @Override
  @JsonProperty
  public String getId()
  {
    return id;
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return "test";
  }

  @JsonProperty
  public TaskStatus getStatus()
  {
    return status;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    return status;
  }
}
