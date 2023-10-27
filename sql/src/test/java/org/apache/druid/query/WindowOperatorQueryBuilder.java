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

import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.operator.OperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.RowSignature;

import java.util.List;
import java.util.Map;

public class WindowOperatorQueryBuilder
{
  private DataSource dataSource;
  private QuerySegmentSpec intervals = new LegacySegmentSpec(Intervals.ETERNITY);
  private Map<String, Object> context;
  private RowSignature rowSignature;
  private List<OperatorFactory> operators;
  private List<OperatorFactory> leafOperators;

  public static WindowOperatorQueryBuilder builder()
  {
    return new WindowOperatorQueryBuilder();
  }

  public WindowOperatorQueryBuilder setDataSource(DataSource dataSource)
  {
    this.dataSource = dataSource;
    return this;
  }

  public WindowOperatorQueryBuilder setDataSource(String dataSource)
  {
    return setDataSource(new TableDataSource(dataSource));
  }

  public WindowOperatorQueryBuilder setDataSource(Query<?> query)
  {
    return setDataSource(new QueryDataSource(query));
  }

  public WindowOperatorQueryBuilder setSignature(RowSignature rowSignature)
  {
    this.rowSignature = rowSignature;
    return this;
  }

  public Query<?> build()
  {
    return new WindowOperatorQuery(
        dataSource,
        intervals,
        context,
        rowSignature,
        operators,
        leafOperators);
  }

  public WindowOperatorQueryBuilder setOperators(OperatorFactory... operators)
  {
    this.operators = Lists.newArrayList(operators);
    return this;
  }

  public WindowOperatorQueryBuilder setLeafOperators(OperatorFactory... operators)
  {
    this.leafOperators = Lists.newArrayList(operators);
    return this;
  }
}
