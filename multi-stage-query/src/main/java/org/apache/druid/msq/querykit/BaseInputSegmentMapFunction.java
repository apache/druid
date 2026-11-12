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

package org.apache.druid.msq.querykit;

import org.apache.druid.query.Query;
import org.apache.druid.query.planning.ExecutionVertex;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.segment.SegmentMapFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@link SegmentMapFunction} for one base input of a leaf stage, associated with the input number of its
 * base datasource.
 */
public record BaseInputSegmentMapFunction(int inputNumber, SegmentMapFunction segmentMapFn)
{
  /**
   * Returns the segment map function for each base input of a query, as {@link BaseInputVertex#createAll} lists
   * them. Everything above a union, such as a join, is built once and shared by every arm.
   *
   * @param query          the stage's query
   * @param policyEnforcer enforcer that every mapped segment must satisfy
   */
  public static List<BaseInputSegmentMapFunction> createAll(final Query<?> query, final PolicyEnforcer policyEnforcer)
  {
    // Both lists are in the order of ExecutionVertex#splitUnion.
    final List<BaseInputVertex> baseInputs = BaseInputVertex.createAll(query);
    final List<SegmentMapFunction> segmentMapFns =
        ExecutionVertex.of(query).splitUnionSegmentMapFunctions(policyEnforcer);

    final List<BaseInputSegmentMapFunction> retVal = new ArrayList<>();
    for (int i = 0; i < baseInputs.size(); i++) {
      retVal.add(new BaseInputSegmentMapFunction(baseInputs.get(i).inputNumber(), segmentMapFns.get(i)));
    }
    return retVal;
  }
}
