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

package io.druid.indexer.path;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.hadoop.DatasourceIngestionSpec;
import io.druid.indexer.hadoop.DatasourceInputFormat;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.timeline.DataSegment;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class DatasourcePathSpec implements PathSpec
{
  private static final Logger logger = new Logger(DatasourcePathSpec.class);

  private final ObjectMapper mapper;
  private final DatasourceIngestionSpec ingestionSpec;
  private final long maxSplitSize;
  private final List<DataSegment> segments;

  public DatasourcePathSpec(
      @JacksonInject ObjectMapper mapper,
      @JsonProperty("segments") List<DataSegment> segments,
      @JsonProperty("ingestionSpec") DatasourceIngestionSpec spec,
      @JsonProperty("maxSplitSize") Long maxSplitSize
  )
  {
    this.mapper = Preconditions.checkNotNull(mapper, "null mapper");
    this.segments = segments;
    this.ingestionSpec = Preconditions.checkNotNull(spec, "null ingestionSpec");

    if(maxSplitSize == null) {
      this.maxSplitSize = 0;
    } else {
      this.maxSplitSize = maxSplitSize.longValue();
    }
  }

  @JsonProperty
  public List<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty
  public DatasourceIngestionSpec getIngestionSpec()
  {
    return ingestionSpec;
  }

  @JsonProperty
  public long getMaxSplitSize()
  {
    return maxSplitSize;
  }

  @Override
  public Job addInputPaths(
      HadoopDruidIndexerConfig config, Job job
  ) throws IOException
  {
    Preconditions.checkArgument(segments != null && !segments.isEmpty(), "no segments provided");

    logger.info(
        "Found total [%d] segments for [%s]  in interval [%s]",
        segments.size(),
        ingestionSpec.getDataSource(),
        ingestionSpec.getInterval()
    );

    DatasourceIngestionSpec updatedIngestionSpec = ingestionSpec;
    if (updatedIngestionSpec.getDimensions() == null) {
      List<String> dims;
      if (config.getParser().getParseSpec().getDimensionsSpec().hasCustomDimensions()) {
        dims = config.getParser().getParseSpec().getDimensionsSpec().getDimensions();
      } else {
        Set<String> dimSet = Sets.newHashSet(
            Iterables.concat(
                Iterables.transform(
                    segments,
                    new Function<DataSegment, Iterable<String>>()
                    {
                      @Override
                      public Iterable<String> apply(DataSegment dataSegment)
                      {
                        return dataSegment.getDimensions();
                      }
                    }
                )
            )
        );
        dims = Lists.newArrayList(
            Sets.difference(
                dimSet,
                config.getParser()
                      .getParseSpec()
                      .getDimensionsSpec()
                      .getDimensionExclusions()
            )
        );
      }
      updatedIngestionSpec = updatedIngestionSpec.withDimensions(dims);
    }

    if (updatedIngestionSpec.getMetrics() == null) {
      Set<String> metrics = Sets.newHashSet();
      final AggregatorFactory[] cols = config.getSchema().getDataSchema().getAggregators();
      if (cols != null) {
        for (AggregatorFactory col : cols) {
          metrics.add(col.getName());
        }
      }
      updatedIngestionSpec = updatedIngestionSpec.withMetrics(Lists.newArrayList(metrics));
    }

    job.getConfiguration().set(DatasourceInputFormat.CONF_DRUID_SCHEMA, mapper.writeValueAsString(updatedIngestionSpec));
    job.getConfiguration().set(DatasourceInputFormat.CONF_INPUT_SEGMENTS, mapper.writeValueAsString(segments));
    job.getConfiguration().set(DatasourceInputFormat.CONF_MAX_SPLIT_SIZE, String.valueOf(maxSplitSize));
    MultipleInputs.addInputPath(job, new Path("/dummy/tobe/ignored"), DatasourceInputFormat.class);

    return job;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DatasourcePathSpec that = (DatasourcePathSpec) o;

    if (maxSplitSize != that.maxSplitSize) {
      return false;
    }
    if (!ingestionSpec.equals(that.ingestionSpec)) {
      return false;
    }
    return !(segments != null ? !segments.equals(that.segments) : that.segments != null);

  }

  @Override
  public int hashCode()
  {
    int result = ingestionSpec.hashCode();
    result = 31 * result + (int) (maxSplitSize ^ (maxSplitSize >>> 32));
    result = 31 * result + (segments != null ? segments.hashCode() : 0);
    return result;
  }
}
