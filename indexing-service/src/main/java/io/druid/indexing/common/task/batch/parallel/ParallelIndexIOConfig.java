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

package io.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.data.input.FirehoseFactory;
import io.druid.indexing.common.task.IndexTask.IndexIOConfig;

import javax.annotation.Nullable;

/**
 * Same with {@link IndexIOConfig} except its JSON type name.
 */
@JsonTypeName("index_parallel")
public class ParallelIndexIOConfig extends IndexIOConfig
{
  public ParallelIndexIOConfig(
      @JsonProperty("firehose") FirehoseFactory firehoseFactory,
      @JsonProperty("appendToExisting") @Nullable Boolean appendToExisting
  )
  {
    super(firehoseFactory, appendToExisting);
  }
}
