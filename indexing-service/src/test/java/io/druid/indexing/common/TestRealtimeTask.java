/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.indexing.common.task.RealtimeIndexTask;
import io.druid.indexing.common.task.TaskResource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.firehose.LocalFirehoseFactory;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.segment.realtime.plumber.PlumberSchool;

import java.io.File;

/**
 */
@JsonTypeName("test_realtime")
public class TestRealtimeTask extends RealtimeIndexTask
{
  private final TaskStatus status;

  @JsonCreator
  public TestRealtimeTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("taskStatus") TaskStatus status
  )
  {
    super(
        id,
        taskResource,
        new FireDepartment(
            new DataSchema(dataSource, null, new AggregatorFactory[]{}, null), new RealtimeIOConfig(
            new LocalFirehoseFactory(new File("lol"), "rofl", null), new PlumberSchool()
        {
          @Override
          public Plumber findPlumber(
              DataSchema schema, RealtimeTuningConfig config, FireDepartmentMetrics metrics
          )
          {
            return null;
          }
        }
        ), null
        )
    );
    this.status = status;
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return "test_realtime";
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    return status;
  }
}
