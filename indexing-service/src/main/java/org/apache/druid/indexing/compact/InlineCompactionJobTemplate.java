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
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.output.OutputDestination;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;

import java.util.List;
import java.util.Objects;

/**
 * Template to create compaction jobs using inline specifications. This template
 * does not fetch any information from the Druid catalog while creating jobs.
 * <p>
 * This template does not contain all the fields supported by
 * {@link InlineSchemaDataSourceCompactionConfig} since some of those fields may
 * change the data itself (and not just its layout) and are thus not considered
 * compaction-compatible.
 */
public class InlineCompactionJobTemplate extends CompactionJobTemplate
{
  public static final String TYPE = "compactInline";

  private final CompactionStateMatcher stateMatcher;

  @JsonCreator
  public InlineCompactionJobTemplate(
      @JsonProperty("stateMatcher") CompactionStateMatcher stateMatcher
  )
  {
    this.stateMatcher = stateMatcher;
  }

  @JsonProperty
  public CompactionStateMatcher getStateMatcher()
  {
    return stateMatcher;
  }

  @Override
  public List<CompactionJob> createCompactionJobs(
      InputSource source,
      OutputDestination destination,
      CompactionJobParams jobParams
  )
  {
    final String dataSource = ensureDruidInputSource(source).getDataSource();
    return CompactionConfigBasedJobTemplate
        .create(dataSource, stateMatcher)
        .createCompactionJobs(source, destination, jobParams);
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
    return Objects.equals(this.stateMatcher, that.stateMatcher);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stateMatcher);
  }

  @Override
  public String getType()
  {
    return TYPE;
  }
}
