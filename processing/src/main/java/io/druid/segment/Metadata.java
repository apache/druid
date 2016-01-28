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

    for (Metadata metadata : toBeMerged) {
      if (metadata != null) {
        foundSomeMetadata = true;
        if (aggregatorsToMerge != null) {
          aggregatorsToMerge.add(metadata.getAggregators());
        }
        mergedContainer.putAll(metadata.container);
      } else {
        //if metadata and hence aggregators for some segment being merged are unknown then
        //final merged segment should not have aggregators in the metadata
        aggregatorsToMerge = null;
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
    return Arrays.equals(aggregators, metadata.aggregators);

  }

  @Override
  public int hashCode()
  {
    int result = container.hashCode();
    result = 31 * result + (aggregators != null ? Arrays.hashCode(aggregators) : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "Metadata{" +
           "container=" + container +
           ", aggregators=" + Arrays.toString(aggregators) +
           '}';
  }
}
