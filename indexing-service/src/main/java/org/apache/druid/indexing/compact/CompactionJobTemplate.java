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

package org.apache.druid.indexing.compact;

import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.output.OutputDestination;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.input.DruidDatasourceDestination;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.indexing.template.BatchIndexingJob;
import org.apache.druid.indexing.template.BatchIndexingJobTemplate;
import org.apache.druid.indexing.template.JobParams;
import org.apache.druid.java.util.common.granularity.Granularity;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Base indexing template for creating {@link CompactionJob}.
 */
public interface CompactionJobTemplate extends BatchIndexingJobTemplate
{
  List<CompactionJob> createCompactionJobs(
      InputSource source,
      OutputDestination destination,
      CompactionJobParams jobParams
  );

  @Nullable
  Granularity getSegmentGranularity();

  @Override
  default List<BatchIndexingJob> createJobs(
      InputSource source,
      OutputDestination destination,
      JobParams jobParams
  )
  {
    if (!(jobParams instanceof CompactionJobParams)) {
      throw InvalidInput.exception(
          "Job params[%s] for compaction template must be of type CompactionJobParams.",
          jobParams
      );
    }
    return createCompactionJobs(source, destination, (CompactionJobParams) jobParams)
        .stream()
        .map(job -> (BatchIndexingJob) job)
        .collect(Collectors.toList());
  }

  /**
   * Verifies that the input source is of type {@link DruidInputSource}.
   */
  default DruidInputSource ensureDruidInputSource(InputSource inputSource)
  {
    if (inputSource instanceof DruidInputSource) {
      return (DruidInputSource) inputSource;
    } else {
      throw InvalidInput.exception("Invalid input source[%s] for compaction", inputSource);
    }
  }

  /**
   * Verifies that the output destination is of type {@link DruidDatasourceDestination}.
   */
  default DruidDatasourceDestination ensureDruidDataSourceDestination(OutputDestination destination)
  {
    if (destination instanceof DruidDatasourceDestination) {
      return (DruidDatasourceDestination) destination;
    } else {
      throw InvalidInput.exception("Invalid output destination[%s] for compaction", destination);
    }
  }
}
