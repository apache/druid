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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.java.util.common.granularity.Granularity;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Template to create compaction jobs using inline specifications. This template
 * does not fetch any information from the Druid catalog while creating jobs.
 */
public class InlineCompactionJobTemplate implements CompactionJobTemplate
{
  public static final String TYPE = "compactInline";

  private final CompactionStateMatcher targetState;

  @JsonCreator
  public InlineCompactionJobTemplate(
      @JsonProperty("targetState") CompactionStateMatcher targetState
  )
  {
    this.targetState = targetState;
  }

  @JsonProperty
  public CompactionStateMatcher getTargetState()
  {
    return targetState;
  }

  @Nullable
  @Override
  public Granularity getSegmentGranularity()
  {
    return targetState.getSegmentGranularity();
  }

  @Override
  public List<CompactionJob> createCompactionJobs(
      DruidInputSource source,
      CompactionJobParams jobParams
  )
  {
    final String dataSource = source.getDataSource();
    return CompactionConfigBasedJobTemplate
        .create(dataSource, targetState)
        .createCompactionJobs(source, jobParams);
  }

  @Override
  public boolean equals(Object object)
  {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    InlineCompactionJobTemplate that = (InlineCompactionJobTemplate) object;
    return Objects.equals(this.targetState, that.targetState);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(targetState);
  }

  @Override
  public String getType()
  {
    return TYPE;
  }
}
