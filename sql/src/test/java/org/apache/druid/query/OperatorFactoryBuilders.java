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

package org.apache.druid.query;

import com.google.common.base.Preconditions;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.operator.ColumnWithDirection.Direction;
import org.apache.druid.query.operator.NaivePartitioningOperatorFactory;
import org.apache.druid.query.operator.NaiveSortOperatorFactory;
import org.apache.druid.query.operator.OffsetLimit;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.ScanOperatorFactory;
import org.apache.druid.query.operator.window.ComposingProcessor;
import org.apache.druid.query.operator.window.Processor;
import org.apache.druid.query.operator.window.WindowFrame;
import org.apache.druid.query.operator.window.WindowFramedAggregateProcessor;
import org.apache.druid.query.operator.window.WindowOperatorFactory;
import org.apache.druid.query.operator.window.ranking.WindowRankProcessor;

import java.util.Arrays;
import java.util.List;

public class OperatorFactoryBuilders
{

  public static ScanOperatorFactoryBuilder scanOperatorFactoryBuilder()
  {
    return new ScanOperatorFactoryBuilder();
  }

  public static class ScanOperatorFactoryBuilder
  {
    private OffsetLimit offsetLimit;
    private DimFilter filter;
    private List<String> projectedColumns;

    public OperatorFactory build()
    {
      return new ScanOperatorFactory(null, filter, offsetLimit, projectedColumns, null, null);
    }

    public ScanOperatorFactoryBuilder setOffsetLimit(long offset, long limit)
    {
      offsetLimit = new OffsetLimit(offset, limit);
      return this;
    }

    public ScanOperatorFactoryBuilder setFilter(DimFilter filter)
    {
      this.filter = filter;
      return this;
    }

    public ScanOperatorFactoryBuilder setProjectedColumns(String... columns)
    {
      this.projectedColumns = Arrays.asList(columns);
      return this;
    }
  }

  public static OperatorFactory naiveSortOperator(ColumnWithDirection... colWithDirs)
  {
    return new NaiveSortOperatorFactory(Arrays.asList(colWithDirs));
  }

  public static OperatorFactory naiveSortOperator(String column, Direction direction)
  {
    return naiveSortOperator(new ColumnWithDirection(column, direction));
  }

  public static OperatorFactory naivePartitionOperator(String... columns)
  {
    return new NaivePartitioningOperatorFactory(Arrays.asList(columns));
  }

  public static WindowOperatorFactory windowOperators(Processor... processors)
  {
    Preconditions.checkArgument(processors.length > 0, "You must specify at least one processor!");
    return new WindowOperatorFactory(processors.length == 1 ? processors[0] : new ComposingProcessor(processors));
  }

  public static Processor rankProcessor(String outputColumn, String... groupingColumns)
  {
    return new WindowRankProcessor(Arrays.asList(groupingColumns), outputColumn, false);
  }

  public static Processor framedAggregateProcessor(WindowFrame window, AggregatorFactory... aggregations)
  {
    return new WindowFramedAggregateProcessor(window, aggregations);
  }
}
