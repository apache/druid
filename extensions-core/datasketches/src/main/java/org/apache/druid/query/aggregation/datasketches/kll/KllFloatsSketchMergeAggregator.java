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

package org.apache.druid.query.aggregation.datasketches.kll;

import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.druid.segment.ColumnValueSelector;

public class KllFloatsSketchMergeAggregator extends KllSketchMergeAggregator<KllFloatsSketch>
{
  public KllFloatsSketchMergeAggregator(final ColumnValueSelector selector, final int k)
  {
    super(selector, KllFloatsSketch.newHeapInstance(k));
  }

  @Override
  public synchronized void aggregate()
  {
    updateUnion(selector, union);
  }

  static void updateUnion(ColumnValueSelector selector, KllFloatsSketch union)
  {
    final Object object = selector.getObject();
    if (object == null) {
      return;
    }
    if (object instanceof KllFloatsSketch) {
      union.merge((KllFloatsSketch) object);
    } else {
      union.update(selector.getFloat());
    }
  }
}
