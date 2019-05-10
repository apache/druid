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

package org.apache.druid.query.movingaverage;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.query.aggregation.PostAggregator;

import java.util.List;
import java.util.Map;

/**
 * Function that can be applied to a Sequence to calculate PostAverager results
 */
public class PostAveragerAggregatorCalculator implements Function<Row, Row>
{

  private final List<PostAggregator> postAveragers;

  public PostAveragerAggregatorCalculator(MovingAverageQuery maq)
  {
    this.postAveragers = maq.getPostAveragerSpecs();
  }

  @Override
  public Row apply(final Row row)
  {
    if (postAveragers.isEmpty()) {
      return row;
    }

    final Map<String, Object> newMap;

    newMap = Maps.newLinkedHashMap(((MapBasedRow) row).getEvent());

    for (PostAggregator postAverager : postAveragers) {
      boolean allColsPresent = postAverager.getDependentFields().stream().allMatch(c -> newMap.get(c) != null);
      newMap.put(postAverager.getName(), allColsPresent ? postAverager.compute(newMap) : null);
    }

    return new MapBasedRow(row.getTimestamp(), newMap);
  }

}
