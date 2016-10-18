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

package io.druid.query.spec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.druid.java.util.common.IAE;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
      intervalStringList = Arrays.asList(intervals.toString());
    } else if (intervals instanceof Map) {
      intervalStringList = (List) ((Map) intervals).get("intervals");
    } else if (intervals instanceof List) {
      intervalStringList = (List) intervals;
    } else {
      throw new IAE("Unknown type[%s] for intervals[%s]", intervals.getClass(), intervals);
    }

    return Lists.transform(
        intervalStringList,
        new Function<Object, Interval>()
        {
          @Override
          public Interval apply(Object input)
          {
            return new Interval(input);
          }
        }
    );
  }

  @JsonCreator
  public LegacySegmentSpec(
      Object intervals
  )
  {
    super(convertValue(intervals));
  }
}
