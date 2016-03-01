/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.histogram;

import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;

public class BufferUtils
{
  public static int binarySearch(DoubleBuffer buf, int minIndex, int maxIndex, double value)
  {
    while (minIndex < maxIndex) {
      int currIndex = (minIndex + maxIndex - 1) >>> 1;

      double currValue = buf.get(currIndex);
      int comparison = Double.compare(currValue, value);
      if (comparison == 0) {
        return currIndex;
      }

      if (comparison < 0) {
        minIndex = currIndex + 1;
      } else {
        maxIndex = currIndex;
      }
    }

    return -(minIndex + 1);
  }

  public static int binarySearch(FloatBuffer buf, int minIndex, int maxIndex, float value)
  {
    while (minIndex < maxIndex) {
      int currIndex = (minIndex + maxIndex - 1) >>> 1;

      float currValue = buf.get(currIndex);
      int comparison = Float.compare(currValue, value);
      if (comparison == 0) {
        return currIndex;
      }

      if (comparison < 0) {
        minIndex = currIndex + 1;
      } else {
        maxIndex = currIndex;
      }
    }

    return -(minIndex + 1);
  }
}
