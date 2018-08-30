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

package org.apache.druid.query.aggregation.histogram;

public class ArrayUtils
{
  public static int hashCode(long[] a, int fromIndex, int toIndex)
  {
    int hashCode = 1;
    int i = fromIndex;
    while (i < toIndex) {
      long v = a[i];
      hashCode = 31 * hashCode + (int) (v ^ (v >>> 32));
      ++i;
    }
    return hashCode;
  }

  public static int hashCode(float[] a, int fromIndex, int toIndex)
  {
    int hashCode = 1;
    int i = fromIndex;
    while (i < toIndex) {
      hashCode = 31 * hashCode + Float.floatToIntBits(a[i]);
      ++i;
    }
    return hashCode;
  }

  public static int hashCode(double[] a, int fromIndex, int toIndex)
  {
    int hashCode = 1;
    int i = fromIndex;
    while (i < toIndex) {
      long v = Double.doubleToLongBits(a[i]);
      hashCode = 31 * hashCode + (int) (v ^ (v >>> 32));
      ++i;
    }
    return hashCode;
  }
}
