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

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class Metadata
{
  // container is used for arbitrary key-value pairs in segment metadata e.g.
  // kafka firehose uses it to store commit offset
  @JsonProperty
  private final Map<String, Object> container;

  @JsonProperty
  private AggregatorFactory[] aggregators;

  @JsonProperty
  private TimestampSpec timestampSpec;

  @JsonProperty
  private QueryGranularity queryGranularity;

  @JsonProperty
  private Boolean rollup;

  public Metadata()
  {
    container = new ConcurrentHashMap<>();
  }

  public AggregatorFactory[] getAggregators()
  {
    return aggregators;
  }

  public Metadata setAggregators(AggregatorFactory[] aggregators)
  {
    this.aggregators = aggregators;
    return this;
  }

  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }

  public Metadata setTimestampSpec(TimestampSpec timestampSpec)
  {
    this.timestampSpec = timestampSpec;
    return this;
  }

  public QueryGranularity getQueryGranularity()
  {
    return queryGranularity;
  }

  public Metadata setQueryGranularity(QueryGranularity queryGranularity)
  {
    this.queryGranularity = queryGranularity;
    return this;
  }

  public Boolean isRollup()
  {
    return rollup;
  }

  public Metadata setRollup(Boolean rollup)
  {
    this.rollup = rollup;
    return this;
  }

  public Metadata putAll(Map<String, Object> other)
  {
    if (other != null) {
      container.putAll(other);
    }
    return this;
  }

  public Object get(String key)
  {
    return container.get(key);
  }

  public Metadata put(String key, Object value)
  {
    if (value != null) {
      container.put(key, value);
    }
    return this;
  }

  // arbitrary key-value pairs from the metadata just follow the semantics of last one wins if same
  // key exists in multiple input Metadata containers
  // for others e.g. Aggregators, appropriate merging is done
  public static Metadata merge(
      List<Metadata> toBeMerged,
      AggregatorFactory[] overrideMergedAggregators
  )
  {
    if (toBeMerged == null || toBeMerged.size() == 0) {
      return null;
    }

    boolean foundSomeMetadata = false;
    Map<String, Object> mergedContainer = new HashMap<>();
    List<AggregatorFactory[]> aggregatorsToMerge = overrideMergedAggregators == null
                                                   ? new ArrayList<AggregatorFactory[]>()
                                                   : null;

    List<TimestampSpec> timestampSpecsToMerge = new ArrayList<>();
    List<QueryGranularity> gransToMerge = new ArrayList<>();
    List<Boolean> rollupToMerge = new ArrayList<>();

    for (Metadata metadata : toBeMerged) {
      if (metadata != null) {
        foundSomeMetadata = true;
        if (aggregatorsToMerge != null) {
          aggregatorsToMerge.add(metadata.getAggregators());
        }

        if (timestampSpecsToMerge != null && metadata.getTimestampSpec() != null) {
          timestampSpecsToMerge.add(metadata.getTimestampSpec());
        }

        if (gransToMerge != null) {
          gransToMerge.add(metadata.getQueryGranularity());
        }

        if (rollupToMerge != null) {
          rollupToMerge.add(metadata.isRollup());
        }
        mergedContainer.putAll(metadata.container);
      } else {
        //if metadata and hence aggregators and queryGranularity for some segment being merged are unknown then
        //final merged segment should not have same in metadata
        aggregatorsToMerge = null;
        timestampSpecsToMerge = null;
        gransToMerge = null;
        rollupToMerge = null;
      }
    }

    if(!foundSomeMetadata) {
      return null;
    }

    Metadata result = new Metadata();
    if (aggregatorsToMerge != null) {
      result.setAggregators(AggregatorFactory.mergeAggregators(aggregatorsToMerge));
    } else {
      result.setAggregators(overrideMergedAggregators);
    }

    if (timestampSpecsToMerge != null) {
      result.setTimestampSpec(TimestampSpec.mergeTimestampSpec(timestampSpecsToMerge));
    }

    if (gransToMerge != null) {
      result.setQueryGranularity(QueryGranularity.mergeQueryGranularities(gransToMerge));
    }

    Boolean rollup = null;
    if (rollupToMerge != null && !rollupToMerge.isEmpty()) {
      rollup = rollupToMerge.get(0);
      for (Boolean r : rollupToMerge) {
        if (r == null) {
          rollup = null;
          break;
        } else if (!r.equals(rollup)) {
          rollup = null;
          break;
        } else {
          rollup = r;
        }
      }
    }

    result.setRollup(rollup);
    result.container.putAll(mergedContainer);
    return result;

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

    Metadata metadata = (Metadata) o;

    if (!container.equals(metadata.container)) {
      return false;
    }
    // Probably incorrect - comparing Object[] arrays with Arrays.equals
    if (!Arrays.equals(aggregators, metadata.aggregators)) {
      return false;
    }
    if (timestampSpec != null ? !timestampSpec.equals(metadata.timestampSpec) : metadata.timestampSpec != null) {
      return false;
    }
    if (rollup != null ? !rollup.equals(metadata.rollup) : metadata.rollup != null) {
      return false;
    }
    return queryGranularity != null
           ? queryGranularity.equals(metadata.queryGranularity)
           : metadata.queryGranularity == null;

  }

  @Override
  public int hashCode()
  {
    int result = container.hashCode();
    result = 31 * result + Arrays.hashCode(aggregators);
    result = 31 * result + (timestampSpec != null ? timestampSpec.hashCode() : 0);
    result = 31 * result + (queryGranularity != null ? queryGranularity.hashCode() : 0);
    result = 31 * result + (rollup != null ? rollup.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "Metadata{" +
           "container=" + container +
           ", aggregators=" + Arrays.toString(aggregators) +
           ", timestampSpec=" + timestampSpec +
           ", queryGranularity=" + queryGranularity +
           ", rollup=" + rollup +
           '}';
  }
}
