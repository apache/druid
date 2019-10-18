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

package org.apache.druid.sql.calcite.expression;

import org.apache.druid.query.aggregation.PostAggregator;

import java.util.ArrayList;
import java.util.List;

/**
 * This class serves as a tracking structure for managing post aggregator column names and any post aggs that
 * are created as part of translation of a Calcite {@code RexNode} into native Druid structures.
 */
public class PostAggregatorVisitor
{
  private String outputNamePrefix;
  private int counter = 0;
  private List<PostAggregator> postAggs = new ArrayList<>();

  public PostAggregatorVisitor(
      String outputNamePrefix
  )
  {
    this.outputNamePrefix = outputNamePrefix;
  }

  public int getAndIncrementCounter()
  {
    return counter++;
  }

  public String getOutputNamePrefix()
  {
    return outputNamePrefix;
  }

  public List<PostAggregator> getPostAggs()
  {
    return postAggs;
  }

  public void addPostAgg(PostAggregator postAggregator)
  {
    postAggs.add(postAggregator);
  }
}
