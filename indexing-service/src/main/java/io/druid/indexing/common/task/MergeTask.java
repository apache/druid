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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.indexing.common.TaskToolbox;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.File;
import java.util.List;
import java.util.Map;

/**
 */
public class MergeTask extends MergeTaskBase
{
  @JsonIgnore
  private final List<AggregatorFactory> aggregators;
  private final Boolean rollup;
  private final IndexSpec indexSpec;

  @JsonCreator
  public MergeTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("segments") List<DataSegment> segments,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregators,
      @JsonProperty("rollup") Boolean rollup,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      // This parameter is left for compatibility when reading existing JSONs, to be removed in Druid 0.12.
      @JsonProperty("buildV9Directly") Boolean buildV9Directly,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(id, dataSource, segments, context);
    this.aggregators = Preconditions.checkNotNull(aggregators, "null aggregations");
    this.rollup = rollup == null ? Boolean.TRUE : rollup;
    this.indexSpec = indexSpec == null ? new IndexSpec() : indexSpec;
  }

  @Override
  public File merge(final TaskToolbox toolbox, final Map<DataSegment, File> segments, final File outDir)
      throws Exception
  {
    IndexMerger indexMerger = toolbox.getIndexMergerV9();
    return indexMerger.mergeQueryableIndex(
        Lists.transform(
            ImmutableList.copyOf(segments.values()),
            new Function<File, QueryableIndex>()
            {
              @Override
              public QueryableIndex apply(@Nullable File input)
              {
                try {
                  return toolbox.getIndexIO().loadIndex(input);
                }
                catch (Exception e) {
                  throw Throwables.propagate(e);
                }
              }
            }
        ),
        rollup,
        aggregators.toArray(new AggregatorFactory[aggregators.size()]),
        outDir,
        indexSpec
    );
  }

  @Override
  public String getType()
  {
    return "merge";
  }

  @JsonProperty
  public Boolean getRollup()
  {
    return rollup;
  }

  @JsonProperty
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  @JsonProperty("aggregations")
  public List<AggregatorFactory> getAggregators()
  {
    return aggregators;
  }
}
