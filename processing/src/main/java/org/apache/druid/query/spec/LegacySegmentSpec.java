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

package org.apache.druid.query.spec;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.druid.java.util.common.IAE;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 */
public class LegacySegmentSpec extends MultipleIntervalSegmentSpec
{
  private static List<Interval> convertValue(Object intervals)
  {
    final List<?> intervalStringList;
    if (intervals instanceof String) {
      intervalStringList = Arrays.asList((((String) intervals).split(",")));
    } else if (intervals instanceof Interval) {
      intervalStringList = Collections.singletonList(intervals.toString());
    } else if (intervals instanceof Map) {
      intervalStringList = (List) ((Map) intervals).get("intervals");
    } else if (intervals instanceof List) {
      intervalStringList = (List) intervals;
    } else {
      throw new IAE("Unknown type[%s] for intervals[%s]", intervals.getClass(), intervals);
    }

    return intervalStringList
        .stream()
        .map(input -> new Interval(input, ISOChronology.getInstanceUTC()))
        .collect(Collectors.toList());
  }

  @JsonCreator
  public LegacySegmentSpec(
      Object intervals
  )
  {
    super(convertValue(intervals));
  }
}
