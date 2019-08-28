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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.IngestionSpec;

public class PartialSegmentMergeIngestionSpec
    extends IngestionSpec<PartialSegmentMergeIOConfig, ParallelIndexTuningConfig>
{
  @JsonCreator
  public PartialSegmentMergeIngestionSpec(
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("ioConfig") PartialSegmentMergeIOConfig ioConfig,
      @JsonProperty("tuningConfig") ParallelIndexTuningConfig tuningConfig
  )
  {
    super(dataSchema, ioConfig, tuningConfig);
  }
}
