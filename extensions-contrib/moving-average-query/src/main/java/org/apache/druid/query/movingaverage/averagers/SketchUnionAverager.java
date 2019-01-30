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

package org.apache.druid.query.movingaverage.averagers;

import com.yahoo.sketches.Family;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.SetOperationBuilder;
import com.yahoo.sketches.theta.Union;
import org.apache.druid.query.aggregation.datasketches.theta.SketchHolder;

public class SketchUnionAverager extends BaseAverager<SketchHolder, SketchHolder>
{

  private int startFrom = 0;
  private final SetOperationBuilder builder;

  public SketchUnionAverager(int numBuckets, String name, String fieldName, int cycleSize, int size)
  {
    super(SketchHolder.class, numBuckets, name, fieldName, cycleSize, false);
    this.builder = SetOperation.builder().setNominalEntries(size);
  }

  @Override
  protected SketchHolder computeResult()
  {
    int cycleSize = getCycleSize();
    Object[] obj = getBuckets();
    int numBuckets = getNumBuckets();

    Union union = (Union) builder.build(Family.UNION);
    for (int i = 0; i < numBuckets; i += cycleSize) {
      if (obj[(i + startFrom) % numBuckets] != null) {
        ((SketchHolder) obj[(i + startFrom) % numBuckets]).updateUnion(union);
      }
    }

    startFrom++;
    return SketchHolder.of(union.getResult());
  }
}
