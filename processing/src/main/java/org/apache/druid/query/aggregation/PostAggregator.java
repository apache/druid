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

package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.java.util.common.Cacheable;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ColumnTypeFactory;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 * Functionally similar to an Aggregator. See the Aggregator interface for more comments.
 */
@ExtensionPoint
public interface PostAggregator extends Cacheable
{
  Set<String> getDependentFields();

  Comparator getComparator();

  @Nullable
  Object compute(Map<String, Object> combinedAggregators);

  @Nullable
  @JsonInclude(Include.NON_NULL)
  String getName();

  /**
   * Return the output type of a row processed with this post aggregator. Refer to the {@link ColumnType} javadocs
   * for details on the implications of choosing a type.
   *
   * @param signature
   */
  @Nullable
  default ColumnType getType(ColumnInspector signature)
  {
    return ColumnTypeFactory.ofValueType(getType());
  }

  /**
   * This method is deprecated and will be removed soon. Use {@link #getType(ColumnInspector)} instead. Do not call this
   * method, it will likely produce incorrect results, it exists for backwards compatibility.
   */
  @Deprecated
  default ValueType getType()
  {
    throw new UnsupportedOperationException(
        "Do not call or implement this method, it is deprecated, use 'getType(ColumnInspector)' instead"
    );
  }


  /**
   * Allows returning an enriched post aggregator, built from contextual information available from the given map of
   * {@link AggregatorFactory} keyed by their names. Callers must call this method before calling {@link #compute} or
   * {@link #getComparator}. This is typically done in the constructor of queries which support post aggregators, via
   * {@link org.apache.druid.query.Queries#prepareAggregations}.
   */
  PostAggregator decorate(Map<String, AggregatorFactory> aggregators);
}
