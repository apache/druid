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

import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.indexing.template.BatchIndexingJobTemplate;
import org.apache.druid.java.util.common.granularity.Granularity;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Base indexing template for creating {@link CompactionJob}.
 */
public interface CompactionJobTemplate extends BatchIndexingJobTemplate
{
  /**
   * Creates compaction jobs with this template for the given datasource.
   */
  List<CompactionJob> createCompactionJobs(
      DruidInputSource source,
      CompactionJobParams jobParams
  );

  /**
   * Granularity of segments created upon successful compaction.
   *
   * @return null only if this template does not change segment granularity upon
   * successful compaction.
   */
  @Nullable
  Granularity getSegmentGranularity();
}
