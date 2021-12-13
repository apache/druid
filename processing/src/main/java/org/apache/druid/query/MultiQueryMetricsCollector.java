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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.guava.GuavaUtils;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MultiQueryMetricsCollector
{
  // subQueryId -> metrics collector
  private final Map<String, SingleQueryMetricsCollector> collectorMap;

  private MultiQueryMetricsCollector(Map<String, SingleQueryMetricsCollector> collectorMap)
  {
    this.collectorMap = collectorMap;
  }

  public static MultiQueryMetricsCollector newCollector()
  {
    return new MultiQueryMetricsCollector(new HashMap<>());
  }

  @JsonCreator
  public static MultiQueryMetricsCollector fromList(final List<SingleQueryMetricsCollector> collectors)
  {
    return new MultiQueryMetricsCollector(
        collectors.stream()
                  .collect(
                      Collectors.toMap(
                          SingleQueryMetricsCollector::getSubQueryId,
                          collector -> collector
                      )
                  )
    );
  }

  public MultiQueryMetricsCollector add(final SingleQueryMetricsCollector otherCollector)
  {
    final String key = otherCollector.getSubQueryId() == null ? "" : otherCollector.getSubQueryId();
    collectorMap.merge(key, otherCollector, SingleQueryMetricsCollector::add);
    return this;
  }

  public MultiQueryMetricsCollector addAll(final MultiQueryMetricsCollector other)
  {
    for (SingleQueryMetricsCollector otherCollector : other.collectorMap.values()) {
      add(otherCollector);
    }

    return this;
  }

  @JsonValue
  public List<SingleQueryMetricsCollector> getCollectors()
  {
    return ImmutableList.copyOf(
        collectorMap
            .values()
            .stream()
            .sorted(
                Comparator.comparing(
                    collector -> {
                      final String subQueryId = collector.getSubQueryId();

                      if (subQueryId == null) {
                        return -2L;
                      } else {
                        // Parsing the subQueryIds that were generated in ClientQuerySegmentWalker.
                        final int lastUnderscore = subQueryId.lastIndexOf('_');

                        if (lastUnderscore >= 0 && lastUnderscore < subQueryId.length() - 1) {
                          final Long n = GuavaUtils.tryParseLong(subQueryId.substring(lastUnderscore + 1));

                          if (n != null) {
                            return n;
                          }
                        }
                      }

                      return -1L;
                    }
                )
            )
            .collect(Collectors.toList()));
  }
}
