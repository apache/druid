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

package org.apache.druid.query.groupby.orderby;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Function;
import org.apache.druid.java.util.common.Cacheable;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;

import javax.annotation.Nullable;
import java.util.Set;

/**
 *
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = NoopLimitSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "default", value = DefaultLimitSpec.class)
})
public interface LimitSpec extends Cacheable
{
  static LimitSpec nullToNoopLimitSpec(@Nullable LimitSpec limitSpec)
  {
    return (limitSpec == null) ? NoopLimitSpec.instance() : limitSpec;
  }

  /**
   * Returns a function that applies a limit to an input sequence that is assumed to be sorted on dimensions.
   *
   * @param query the query that this limit spec belongs to
   *
   * @return limit function
   */
  Function<Sequence<ResultRow>, Sequence<ResultRow>> build(GroupByQuery query);

  LimitSpec merge(LimitSpec other);

  /**
   * Discard sorting columns not contained in given set. This is used when generating new queries, e.g. to process
   * subtotal spec in GroupBy query.
   *
   * @param names columns names to keep
   * @return new LimitSpec that works with fitlered set of columns
   */
  LimitSpec filterColumns(Set<String> names);
}
