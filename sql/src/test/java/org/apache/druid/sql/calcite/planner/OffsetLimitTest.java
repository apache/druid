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

package org.apache.druid.sql.calcite.planner;

import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class OffsetLimitTest
{
  @Test
  public void testAndThen()
  {
    final List<String> things = ImmutableList.of("a", "b", "c", "d", "e", "f", "g", "h");

    for (int innerOffset = 0; innerOffset < 5; innerOffset++) {
      for (int innerLimit = -1; innerLimit < 5; innerLimit++) {
        for (int outerOffset = 0; outerOffset < 5; outerOffset++) {
          for (int outerLimit = -1; outerLimit < 5; outerLimit++) {
            final OffsetLimit inner = new OffsetLimit(innerOffset, innerLimit < 0 ? null : (long) innerLimit);
            final OffsetLimit outer = new OffsetLimit(outerOffset, outerLimit < 0 ? null : (long) outerLimit);
            final OffsetLimit combined = inner.andThen(outer);

            Assert.assertEquals(
                StringUtils.format(
                    "innerOffset[%s], innerLimit[%s], outerOffset[%s], outerLimit[%s]",
                    innerOffset,
                    innerLimit,
                    outerOffset,
                    outerLimit
                ),
                things.stream()
                      .skip(innerOffset)
                      .limit(innerLimit < 0 ? Long.MAX_VALUE : innerLimit)
                      .skip(outerOffset)
                      .limit(outerLimit < 0 ? Long.MAX_VALUE : outerLimit)
                      .collect(Collectors.toList()),
                things.stream()
                      .skip(combined.getOffset())
                      .limit(combined.hasLimit() ? combined.getLimit() : Long.MAX_VALUE)
                      .collect(Collectors.toList())
            );
          }
        }
      }
    }
  }
}
