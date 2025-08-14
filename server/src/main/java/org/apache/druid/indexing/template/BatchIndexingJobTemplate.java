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

package org.apache.druid.indexing.template;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.output.OutputDestination;

import java.util.List;

/**
 * ETL template to create a {@link BatchIndexingJob} that indexes data from an
 * {@link InputSource} into an {@link OutputDestination}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface BatchIndexingJobTemplate
{
  /**
   * Creates jobs with this template for the given input and output.
   */
  List<BatchIndexingJob> createJobs(
      InputSource source,
      OutputDestination destination,
      JobParams jobParams
  );

  /**
   * Unique type name of this template used for JSON serialization.
   */
  @JsonProperty
  String getType();
}
