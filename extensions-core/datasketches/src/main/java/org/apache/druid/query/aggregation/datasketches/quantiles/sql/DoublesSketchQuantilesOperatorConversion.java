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

package org.apache.druid.query.aggregation.datasketches.quantiles.sql;

import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchToQuantilesPostAggregator;

public class DoublesSketchQuantilesOperatorConversion extends DoublesSketchListArgBaseOperatorConversion
{
  private static final String FUNCTION_NAME = "DS_GET_QUANTILES";

  @Override
  public String getFunctionName()
  {
    return FUNCTION_NAME;
  }

  @Override
  public PostAggregator makePostAgg(String name, PostAggregator field, double[] args)
  {
    return new DoublesSketchToQuantilesPostAggregator(
        name,
        field,
        args
    );
  }
}
