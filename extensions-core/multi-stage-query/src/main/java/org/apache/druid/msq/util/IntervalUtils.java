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

package org.apache.druid.msq.util;

import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;

/**
 * Things that would make sense in {@link org.apache.druid.java.util.common.Intervals} if this were not an extension.
 */
public class IntervalUtils
{
  public static List<Interval> difference(final List<Interval> list1, final List<Interval> list2)
  {
    final List<Interval> retVal = new ArrayList<>();

    int i = 0, j = 0;
    while (i < list1.size()) {
      while (j < list2.size() && list2.get(j).isBefore(list1.get(i))) {
        j++;
      }

      if (j == list2.size() || list2.get(j).isAfter(list1.get(i))) {
        retVal.add(list1.get(i));
        i++;
      } else {
        final Interval overlap = list1.get(i).overlap(list2.get(j));
        final Interval a = new Interval(list1.get(i).getStart(), overlap.getStart());
        final Interval b = new Interval(overlap.getEnd(), list1.get(i).getEnd());

        if (a.toDurationMillis() > 0) {
          retVal.add(a);
        }

        if (b.toDurationMillis() > 0) {
          list1.set(i, b);
        } else {
          i++;
        }
      }
    }

    return retVal;
  }
}
