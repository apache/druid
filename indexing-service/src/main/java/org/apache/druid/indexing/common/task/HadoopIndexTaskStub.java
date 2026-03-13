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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * 'index_hadoop' {@link Task} was removed in Druid 37, however we retain this stub for serde reasons and error logging
 * reasons in the event we come across any of these tasks.
 */
@Deprecated
public class HadoopIndexTaskStub extends AbstractBatchIndexTask
{
  public static final String TYPE = "index_hadoop";

  private final String id;
  private final Map<String, Object> spec;
  private final List<String> hadoopDependencyCoordinates;
  private final String classpathPrefix;
  private final Map<String, Object> context;

  @JsonCreator
  public HadoopIndexTaskStub(
      @JsonProperty("id") String id,
      @JsonProperty("spec") Map<String, Object> spec,
      @JsonProperty("hadoopCoordinates") String hadoopCoordinates,
      @JsonProperty("hadoopDependencyCoordinates") List<String> hadoopDependencyCoordinates,
      @JsonProperty("classpathPrefix") String classpathPrefix,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        AbstractTask.getOrMakeId(id, TYPE, getTheDataSource(spec)),
        getTheDataSource(spec),
        context,
        IngestionMode.NONE
    );
    this.id = id;
    this.spec = spec;
    this.hadoopDependencyCoordinates = hadoopCoordinates != null
                                       ? List.of(hadoopCoordinates)
                                       : hadoopDependencyCoordinates;
    this.classpathPrefix = classpathPrefix;
    this.context = context;
  }

  @JsonProperty("spec")
  public Map<String, Object> getSpec()
  {
    return spec;
  }

  @JsonProperty
  public List<String> getHadoopDependencyCoordinates()
  {
    return hadoopDependencyCoordinates;
  }

  @JsonProperty
  @Override
  public String getClasspathPrefix()
  {
    return classpathPrefix;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    throw noHadoop();
  }

  @Override
  public boolean requireLockExistingSegments()
  {
    throw noHadoop();
  }

  @Override
  public List<DataSegment> findSegmentsToLock(TaskActionClient taskActionClient, List<Interval> intervals)
  {
    throw noHadoop();
  }

  @Override
  public boolean isPerfectRollup()
  {
    throw noHadoop();
  }

  @Nullable
  @Override
  public Granularity getSegmentGranularity()
  {
    throw noHadoop();
  }

  @Override
  public TaskStatus runTask(TaskToolbox toolbox)
  {
    throw noHadoop();
  }

  @Override
  protected Map<String, Object> getTaskCompletionRowStats()
  {
    throw noHadoop();
  }

  private static String getTheDataSource(Map<String, Object> spec)
  {
    final Map<String, Object> dataSchema = (Map<String, Object>) spec.get("dataSchema");
    if (dataSchema != null) {
      return (String) dataSchema.get("dataSource");
    }
    throw DruidException.defensive("cannot find dataSchema.dataSource for task");
  }

  private static RuntimeException noHadoop()
  {
    return DruidException.defensive("The Apache Hadoop ingestion task was removed in Druid 37.0");
  }
}
