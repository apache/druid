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

package org.apache.druid.query.lookbackquery;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 * A wrapper class to rename given PostAggregator
 */
public class PostAggregatorWrapper implements PostAggregator
{

  public final PostAggregator postAggregator;
  public final String prefix;

  public PostAggregatorWrapper(PostAggregator pa, String prefix)
  {
    this.postAggregator = pa;
    this.prefix = prefix;

  }

  @Override
  public Set<String> getDependentFields()
  {
    return postAggregator.getDependentFields();
  }

  @Override
  public Comparator getComparator()
  {
    return postAggregator.getComparator();
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    return postAggregator.compute(combinedAggregators);
  }

  /**
   * Returns PostAggregator name prefixed with prefix
   */
  @Override
  public String getName()
  {
    return prefix + postAggregator.getName();
  }

  @Override
  public byte[] getCacheKey()
  {
    return postAggregator.getCacheKey();
  }

  @Override
  public PostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    return postAggregator.decorate(aggregators);
  }

}
