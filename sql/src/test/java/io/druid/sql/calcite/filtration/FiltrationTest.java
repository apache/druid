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

package io.druid.sql.calcite.filtration;

import com.google.common.collect.ImmutableList;
import io.druid.java.util.common.Intervals;
import io.druid.query.filter.IntervalDimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.segment.column.Column;
import org.junit.Assert;
import org.junit.Test;

public class FiltrationTest
{
  @Test
  public void testNotIntervals()
  {
    final Filtration filtration = Filtration.create(
        new NotDimFilter(
            new IntervalDimFilter(
                Column.TIME_COLUMN_NAME,
                ImmutableList.of(Intervals.of("2000/2001"), Intervals.of("2002/2003")),
                null
            )
        ),
        null
    ).optimize(null);

    Assert.assertEquals(
        ImmutableList.of(Filtration.eternity()),
        filtration.getIntervals()
    );

    Assert.assertEquals(
        new NotDimFilter(
            new IntervalDimFilter(
                Column.TIME_COLUMN_NAME,
                ImmutableList.of(Intervals.of("2000/2001"), Intervals.of("2002/2003")),
                null
            )
        ),
        filtration.getDimFilter()
    );
  }
}
