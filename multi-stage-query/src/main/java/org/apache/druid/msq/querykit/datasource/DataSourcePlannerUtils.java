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

package org.apache.druid.msq.querykit.datasource;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.msq.querykit.DataSourcePlanner;
import org.apache.druid.msq.querykit.InputNumberDataSource;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.ColumnHolder;

import java.util.stream.Collectors;

/**
 * Utilities shared by {@link DataSourcePlanner} implementations.
 */
public class DataSourcePlannerUtils
{
  /**
   * Shift every {@link InputNumberDataSource} in the provided datasource tree upwards by the given amount. Used when
   * merging the inputs of multiple child plans into a single plan.
   */
  public static DataSource shiftInputNumbers(final DataSource dataSource, final int shift)
  {
    if (shift < 0) {
      throw new IAE("Shift must be >= 0");
    } else if (shift == 0) {
      return dataSource;
    } else {
      if (dataSource instanceof InputNumberDataSource) {
        return new InputNumberDataSource(((InputNumberDataSource) dataSource).getInputNumber() + shift);
      } else {
        return dataSource.withChildren(
            dataSource.getChildren()
                      .stream()
                      .map(child -> shiftInputNumbers(child, shift))
                      .collect(Collectors.toList())
        );
      }
    }
  }

  /**
   * Verify that the provided {@link QuerySegmentSpec} is a {@link MultipleIntervalSegmentSpec} with
   * interval {@link Intervals#ETERNITY}. If not, throw an {@link UnsupportedOperationException}.
   * <p>
   * See {@link org.apache.druid.sql.calcite.rel.DruidQuery#canUseIntervalFiltering(DataSource)}.
   */
  public static void checkQuerySegmentSpecIsEternity(
      final DataSource dataSource,
      final QuerySegmentSpec querySegmentSpec
  )
  {
    final boolean querySegmentSpecIsEternity =
        querySegmentSpec instanceof MultipleIntervalSegmentSpec
        && querySegmentSpec.getIntervals().equals(Intervals.ONLY_ETERNITY);

    if (!querySegmentSpecIsEternity) {
      throw new UOE(
          "Cannot filter datasource [%s] using [%s]",
          dataSource.getClass().getName(),
          ColumnHolder.TIME_COLUMN_NAME
      );
    }
  }
}
