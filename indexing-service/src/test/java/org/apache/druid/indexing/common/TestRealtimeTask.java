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

package org.apache.druid.indexing.common;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.RealtimeIndexTask;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.firehose.LocalFirehoseFactory;

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
      @JsonProperty("taskStatus") TaskStatus status,
      @JacksonInject ObjectMapper mapper
  )
  {
    super(
        id,
        taskResource,
        new FireDepartment(
            new DataSchema(dataSource, null, new AggregatorFactory[]{}, null, null, mapper),
            new RealtimeIOConfig(
                new LocalFirehoseFactory(new File("lol"), "rofl", null),
                (schema, config, metrics) -> null
            ),
            null
        ),
        null
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
  public TaskStatus run(TaskToolbox toolbox)
  {
    return status;
  }
}
