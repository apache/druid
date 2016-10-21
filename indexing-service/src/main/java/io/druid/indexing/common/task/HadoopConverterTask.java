/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import io.druid.indexer.updater.HadoopConverterJob;
import io.druid.indexer.updater.HadoopDruidConverterConfig;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.java.util.common.UOE;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.IndexSpec;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class HadoopConverterTask extends ConvertSegmentTask
{
  private static final String TYPE = "hadoop_convert_segment";
  private static final Logger log = new Logger(HadoopConverterTask.class);

  @JsonCreator
  public HadoopConverterTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      @JsonProperty("force") boolean force,
      @JsonProperty("validate") Boolean validate,
      @JsonProperty("hadoopDependencyCoordinates") List<String> hadoopDependencyCoordinates,
      @JsonProperty("distributedSuccessCache") URI distributedSuccessCache,
      @JsonProperty("jobPriority") String jobPriority,
      @JsonProperty("segmentOutputPath") String segmentOutputPath,
      @JsonProperty("classpathPrefix") String classpathPrefix,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        makeId(
            id,
            TYPE,
            Preconditions.checkNotNull(dataSource, "dataSource"),
            Preconditions.checkNotNull(interval, "interval")
        ),
        dataSource,
        interval,
        null, // Always call subtask codepath
        indexSpec,
        force,
        validate == null ? true : validate,
        context
    );
    this.hadoopDependencyCoordinates = hadoopDependencyCoordinates;
    this.distributedSuccessCache = Preconditions.checkNotNull(distributedSuccessCache, "distributedSuccessCache");
    this.segmentOutputPath = Preconditions.checkNotNull(segmentOutputPath, "segmentOutputPath");
    this.jobPriority = jobPriority;
    this.classpathPrefix = classpathPrefix;
  }

  private final List<String> hadoopDependencyCoordinates;
  private final URI distributedSuccessCache;
  private final String jobPriority;
  private final String segmentOutputPath;
  private final String classpathPrefix;

  @JsonProperty
  public List<String> getHadoopDependencyCoordinates()
  {
    return hadoopDependencyCoordinates;
  }

  @JsonProperty
  public URI getDistributedSuccessCache()
  {
    return distributedSuccessCache;
  }

  @JsonProperty
  public String getJobPriority()
  {
    return jobPriority;
  }

  @JsonProperty
  public String getSegmentOutputPath()
  {
    return segmentOutputPath;
  }

  @Override
  @JsonProperty
  public String getClasspathPrefix()
  {
    return classpathPrefix;
  }

  @Override
  protected Iterable<Task> generateSubTasks(
      final String groupId,
      final Iterable<DataSegment> segments,
      final IndexSpec indexSpec,
      final boolean force,
      final boolean validate,
      Map<String, Object> context
  )
  {
    return Collections.<Task>singleton(
        new ConverterSubTask(
            ImmutableList.copyOf(segments),
            this,
            context
        )
    );
  }

  @Override
  @JsonIgnore
  public DataSegment getSegment()
  {
    throw new UOE(
        "Sub-less data segment not supported for hadoop converter task. Specify interval and datasource instead"
    );
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  public static class ConverterSubTask extends HadoopTask
  {
    private final List<DataSegment> segments;
    private final HadoopConverterTask parent;

    @JsonCreator
    public ConverterSubTask(
        @JsonProperty("segments") List<DataSegment> segments,
        @JsonProperty("parent") HadoopConverterTask parent,
        @JsonProperty("context") Map<String, Object> context
    )
    {
      super(
          joinId(
              Preconditions.checkNotNull(parent, "parent").getGroupId(),
              "sub",
              parent.getInterval().getStart(),
              parent.getInterval().getEnd()
          ),
          parent.getDataSource(),
          parent.getHadoopDependencyCoordinates(),
          context
      );
      this.segments = segments;
      this.parent = parent;
    }

    @JsonProperty
    public List<DataSegment> getSegments()
    {
      return segments;
    }

    @JsonProperty
    public HadoopConverterTask getParent()
    {
      return parent;
    }

    @Override
    public String getType()
    {
      return TYPE + "_sub";
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient) throws Exception
    {
      return true;
    }

    @Override
    public TaskStatus run(TaskToolbox toolbox) throws Exception
    {
      final Map<String, String> hadoopProperties = new HashMap<>();
      final Properties properties = injector.getInstance(Properties.class);
      for (String name : properties.stringPropertyNames()) {
        if (name.startsWith("hadoop.")) {
          hadoopProperties.put(name.substring("hadoop.".length()), properties.getProperty(name));
        }
      }
      final ClassLoader loader = buildClassLoader(toolbox);
      final HadoopDruidConverterConfig config = new HadoopDruidConverterConfig(
          getDataSource(),
          parent.getInterval(),
          parent.getIndexSpec(),
          segments,
          parent.isValidate(),
          parent.getDistributedSuccessCache(),
          hadoopProperties,
          parent.getJobPriority(),
          parent.getSegmentOutputPath()
      );

      final String finishedSegmentString = invokeForeignLoader(
          "io.druid.indexing.common.task.HadoopConverterTask$JobInvoker",
          new String[]{HadoopDruidConverterConfig.jsonMapper.writeValueAsString(config)},
          loader
      );
      if (finishedSegmentString == null) {
        return TaskStatus.failure(getId());
      }
      final List<DataSegment> finishedSegments = HadoopDruidConverterConfig.jsonMapper.readValue(
          finishedSegmentString,
          new TypeReference<List<DataSegment>>()
          {
          }
      );
      log.debug("Found new segments %s", Arrays.toString(finishedSegments.toArray()));
      toolbox.publishSegments(finishedSegments);
      return success();
    }
  }

  public static class JobInvoker
  {
    public static String runTask(String[] input)
    {
      final HadoopDruidConverterConfig config;
      try {
        config = HadoopDruidConverterConfig.jsonMapper.readValue(
            input[0],
            HadoopDruidConverterConfig.class
        );
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
      final HadoopConverterJob hadoopConverterJob = new HadoopConverterJob(config);
      try {
        final List<DataSegment> result = hadoopConverterJob.run();
        return result == null
               ? null
               : HadoopDruidConverterConfig.jsonMapper.writeValueAsString(result);
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
