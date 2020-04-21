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

package org.apache.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.indexer.hadoop.DatasourceIngestionSpec;
import org.apache.druid.indexer.hadoop.WindowedDataSegment;
import org.apache.druid.indexer.path.UsedSegmentsRetriever;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.IngestionSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class HadoopIngestionSpec extends IngestionSpec<HadoopIOConfig, HadoopTuningConfig>
{
  private final DataSchema dataSchema;
  private final HadoopIOConfig ioConfig;
  private final HadoopTuningConfig tuningConfig;

  //this is used in the temporary paths on the hdfs unique to an hadoop indexing task
  private final String uniqueId;

  @JsonCreator
  public HadoopIngestionSpec(
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("ioConfig") HadoopIOConfig ioConfig,
      @JsonProperty("tuningConfig") @Nullable HadoopTuningConfig tuningConfig,
      @JsonProperty("uniqueId") @Nullable String uniqueId
  )
  {
    super(dataSchema, ioConfig, tuningConfig);

    this.dataSchema = dataSchema;
    this.ioConfig = ioConfig;
    this.tuningConfig = tuningConfig == null ? HadoopTuningConfig.makeDefaultTuningConfig() : tuningConfig;
    this.uniqueId = uniqueId == null ? UUIDUtils.generateUuid() : uniqueId;
  }

  //for unit tests
  public HadoopIngestionSpec(
      DataSchema dataSchema,
      HadoopIOConfig ioConfig,
      HadoopTuningConfig tuningConfig
  )
  {
    this(dataSchema, ioConfig, tuningConfig, null);
  }

  @JsonProperty("dataSchema")
  @Override
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty("ioConfig")
  @Override
  public HadoopIOConfig getIOConfig()
  {
    return ioConfig;
  }

  @JsonProperty("tuningConfig")
  @Override
  public HadoopTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty("uniqueId")
  public String getUniqueId()
  {
    return uniqueId;
  }

  public HadoopIngestionSpec withDataSchema(DataSchema schema)
  {
    return new HadoopIngestionSpec(
        schema,
        ioConfig,
        tuningConfig,
        uniqueId
    );
  }

  public HadoopIngestionSpec withIOConfig(HadoopIOConfig config)
  {
    return new HadoopIngestionSpec(
        dataSchema,
        config,
        tuningConfig,
        uniqueId
    );
  }

  public HadoopIngestionSpec withTuningConfig(HadoopTuningConfig config)
  {
    return new HadoopIngestionSpec(
        dataSchema,
        ioConfig,
        config,
        uniqueId
    );
  }

  public static void updateSegmentListIfDatasourcePathSpecIsUsed(
      HadoopIngestionSpec spec,
      ObjectMapper jsonMapper,
      UsedSegmentsRetriever segmentsRetriever
  )
      throws IOException
  {
    String dataSource = "dataSource";
    String type = "type";
    String multi = "multi";
    String children = "children";
    String segments = "segments";
    String ingestionSpec = "ingestionSpec";

    Map<String, Object> pathSpec = spec.getIOConfig().getPathSpec();
    List<Map<String, Object>> datasourcePathSpecs = new ArrayList<>();
    if (pathSpec.get(type).equals(dataSource)) {
      datasourcePathSpecs.add(pathSpec);
    } else if (pathSpec.get(type).equals(multi)) {
      List<Map<String, Object>> childPathSpecs = (List<Map<String, Object>>) pathSpec.get(children);
      for (Map<String, Object> childPathSpec : childPathSpecs) {
        if (childPathSpec.get(type).equals(dataSource)) {
          datasourcePathSpecs.add(childPathSpec);
        }
      }
    }

    for (Map<String, Object> datasourcePathSpec : datasourcePathSpecs) {
      Map<String, Object> ingestionSpecMap = (Map<String, Object>) datasourcePathSpec.get(ingestionSpec);
      DatasourceIngestionSpec ingestionSpecObj = jsonMapper.convertValue(
          ingestionSpecMap,
          DatasourceIngestionSpec.class
      );

      Collection<DataSegment> usedVisibleSegments = segmentsRetriever.retrieveUsedSegmentsForIntervals(
          ingestionSpecObj.getDataSource(),
          ingestionSpecObj.getIntervals(),
          Segments.ONLY_VISIBLE
      );

      if (ingestionSpecObj.getSegments() != null) {
        //ensure that user supplied segment list matches with the usedVisibleSegments obtained from db
        //this safety check lets users do test-n-set kind of batch delta ingestion where the delta
        //ingestion task would only run if current state of the system is same as when they submitted
        //the task.
        List<DataSegment> userSuppliedSegmentsList = ingestionSpecObj.getSegments();

        if (usedVisibleSegments.size() == userSuppliedSegmentsList.size()) {
          Set<DataSegment> segmentsSet = new HashSet<>(usedVisibleSegments);

          for (DataSegment userSegment : userSuppliedSegmentsList) {
            if (!segmentsSet.contains(userSegment)) {
              throw new IOException("user supplied segments list did not match with segments list obtained from db");
            }
          }
        } else {
          throw new IOException("user supplied segments list did not match with segments list obtained from db");
        }
      }

      final VersionedIntervalTimeline<String, DataSegment> timeline =
          VersionedIntervalTimeline.forSegments(usedVisibleSegments);
      final List<WindowedDataSegment> windowedSegments = new ArrayList<>();
      for (Interval interval : ingestionSpecObj.getIntervals()) {
        final List<TimelineObjectHolder<String, DataSegment>> timeLineSegments = timeline.lookup(interval);

        for (TimelineObjectHolder<String, DataSegment> holder : timeLineSegments) {
          for (PartitionChunk<DataSegment> chunk : holder.getObject()) {
            windowedSegments.add(new WindowedDataSegment(chunk.getObject(), holder.getInterval()));
          }
        }
        datasourcePathSpec.put(segments, windowedSegments);
      }
    }
  }

}
