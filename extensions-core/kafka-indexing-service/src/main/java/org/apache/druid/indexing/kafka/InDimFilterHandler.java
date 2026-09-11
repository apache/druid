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

package org.apache.druid.indexing.kafka;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.filter.InDimFilter;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

/**
 * Handler for InDimFilter in Kafka header evaluation.
 *
 * This handler evaluates whether a header value is contained in the filter's value set.
 * It uses a HashSet for O(1) average-case lookup performance instead of the filter's
 * internal TreeSet which has O(log n) lookup time.
 */
public class InDimFilterHandler implements HeaderFilterHandler
{
  private final InDimFilter filter;
  private final Set<String> filterValues;

  /**
   * Creates a new handler for the given InDimFilter.
   *
   * @param filter the InDimFilter to handle, must not be null
   * @throws IllegalArgumentException if filter is null
   */
  public InDimFilterHandler(InDimFilter filter)
  {
    this.filter = Preconditions.checkNotNull(filter, "filter cannot be null");

    // Convert to HashSet for O(1) lookups instead of O(log n) TreeSet lookups
    // This optimization is particularly beneficial when the filter has many values
    this.filterValues = new HashSet<>(filter.getValues());
  }

  @Override
  public String getHeaderName()
  {
    return filter.getDimension();
  }

  @Override
  public boolean shouldInclude(@Nullable String headerValue)
  {
    return filterValues.contains(headerValue);
  }

  @Override
  public String getDescription()
  {
    return StringUtils.format(
        "InDimFilter[header=%s, values=%d]",
        filter.getDimension(),
        filterValues.size()
    );
  }
}
