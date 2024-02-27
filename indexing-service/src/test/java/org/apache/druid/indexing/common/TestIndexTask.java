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
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.DataSchema;

import java.io.File;

/**
 */
@JsonTypeName("test_index")
public class TestIndexTask extends IndexTask
{
  private final TaskStatus status;

  @JsonCreator
  public TestIndexTask(
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
        new IndexIngestionSpec(
            new DataSchema(dataSource, null, new AggregatorFactory[]{}, null, null, mapper),
            new IndexTask.IndexIOConfig(
                null,
                new LocalInputSource(new File("lol"), "rofl"),
                new JsonInputFormat(null, null, null, null, null),
                false,
                false
            ),

            new IndexTask.IndexTuningConfig(
                null,
                null,
                null,
                10,
                null,
                null,
                null,
                null,
                null,
                null,
                new DynamicPartitionsSpec(10000, null),
                IndexSpec.DEFAULT,
                null,
                3,
                false,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
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
  public TaskStatus runTask(TaskToolbox toolbox)
  {
    return status;
  }
}
