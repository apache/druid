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

package org.apache.druid.indexer.updater;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.server.DruidNode;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class HadoopDruidConverterConfig
{
  public static final String CONFIG_PROPERTY = "org.apache.druid.indexer.updater.converter";
  public static final ObjectMapper jsonMapper;
  public static final IndexIO INDEX_IO;
  public static final IndexMerger INDEX_MERGER;
  public static final DataSegmentPusher DATA_SEGMENT_PUSHER;

  private static final Injector injector = Initialization.makeInjectorWithModules(
      GuiceInjectors.makeStartupInjector(),
      ImmutableList.<Module>of(
          new Module()
          {
            @Override
            public void configure(Binder binder)
            {
              JsonConfigProvider.bindInstance(
                  binder, Key.get(DruidNode.class, Self.class), new DruidNode("hadoop-converter", null, null, null, true, false)
              );
            }
          }
      )
  );

  static {
    jsonMapper = injector.getInstance(ObjectMapper.class);
    jsonMapper.registerSubtypes(HadoopDruidConverterConfig.class);
    INDEX_IO = injector.getInstance(IndexIO.class);
    INDEX_MERGER = injector.getInstance(IndexMerger.class);
    DATA_SEGMENT_PUSHER = injector.getInstance(DataSegmentPusher.class);
  }

  public static HadoopDruidConverterConfig fromString(final String string) throws IOException
  {
    return fromMap(jsonMapper.readValue(string, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT));
  }

  public static HadoopDruidConverterConfig fromMap(final Map<String, Object> map)
  {
    return jsonMapper.convertValue(map, HadoopDruidConverterConfig.class);
  }

  @JsonProperty
  private final String dataSource;
  @JsonProperty
  private final Interval interval;
  @JsonProperty
  private final IndexSpec indexSpec;
  @JsonProperty
  private final List<DataSegment> segments;
  @JsonProperty
  private final boolean validate;
  @JsonProperty
  private final URI distributedSuccessCache;
  @JsonProperty
  private final Map<String, String> hadoopProperties;
  @JsonProperty
  private final String jobPriority;
  @JsonProperty
  private final String segmentOutputPath;

  @JsonCreator
  public HadoopDruidConverterConfig(
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("interval") final Interval interval,
      @JsonProperty("indexSpec") final IndexSpec indexSpec,
      @JsonProperty("segments") final List<DataSegment> segments,
      @JsonProperty("validate") final Boolean validate,
      @JsonProperty("distributedSuccessCache") URI distributedSuccessCache,
      @JsonProperty("hadoopProperties") Map<String, String> hadoopProperties,
      @JsonProperty("jobPriority") String jobPriority,
      @JsonProperty("segmentOutputPath") String segmentOutputPath
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.interval = Preconditions.checkNotNull(interval, "interval");
    this.indexSpec = Preconditions.checkNotNull(indexSpec, "indexSpec");
    this.distributedSuccessCache = Preconditions.checkNotNull(distributedSuccessCache, "distributedSuccessCache");
    this.segments = segments;
    this.validate = validate == null ? false : validate;
    this.hadoopProperties = hadoopProperties == null
                            ? ImmutableMap.of()
                            : ImmutableMap.copyOf(hadoopProperties);
    this.jobPriority = jobPriority;
    this.segmentOutputPath = Preconditions.checkNotNull(segmentOutputPath, "segmentOutputPath");
  }

  @JsonProperty
  public boolean isValidate()
  {
    return validate;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  @JsonProperty
  public List<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty
  public URI getDistributedSuccessCache()
  {
    return distributedSuccessCache;
  }

  @JsonProperty
  public Map<String, String> getHadoopProperties()
  {
    return hadoopProperties;
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
}
