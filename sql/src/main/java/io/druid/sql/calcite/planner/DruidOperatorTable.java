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

package io.druid.sql.calcite.planner;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DruidOperatorTable implements SqlOperatorTable
{
  private static final SqlStdOperatorTable STANDARD_TABLE = SqlStdOperatorTable.instance();

  private final Map<String, SqlAggregator> aggregators;
  private final Map<String, SqlOperatorConversion> extractionOperators;

  @Inject
  public DruidOperatorTable(
      final Set<SqlAggregator> aggregators,
      final Set<SqlOperatorConversion> extractionOperators
  )
  {
    this.aggregators = Maps.newHashMap();
    this.extractionOperators = Maps.newHashMap();

    for (SqlAggregator aggregator : aggregators) {
      final String lcname = StringUtils.toLowerCase(aggregator.calciteFunction().getName());
      if (this.aggregators.put(lcname, aggregator) != null) {
        throw new ISE("Cannot have two operators with name[%s]", lcname);
      }
    }

    for (SqlOperatorConversion extractionFunction : extractionOperators) {
      final String lcname = StringUtils.toLowerCase(extractionFunction.calciteOperator().getName());
      if (this.aggregators.containsKey(lcname) || this.extractionOperators.put(lcname, extractionFunction) != null) {
        throw new ISE("Cannot have two operators with name[%s]", lcname);
      }
    }
  }

  public SqlAggregator lookupAggregator(final SqlAggFunction aggFunction)
  {
    final SqlAggregator sqlAggregator = aggregators.get(StringUtils.toLowerCase(aggFunction.getName()));
    if (sqlAggregator != null && sqlAggregator.calciteFunction().equals(aggFunction)) {
      return sqlAggregator;
    } else {
      return null;
    }
  }

  public SqlOperatorConversion lookupOperatorConversion(final SqlOperator operator)
  {
    final SqlOperatorConversion operatorConversion = extractionOperators.get(StringUtils.toLowerCase(operator.getName()));
    if (operatorConversion != null && operatorConversion.calciteOperator().equals(operator)) {
      return operatorConversion;
    } else {
      return null;
    }
  }

  @Override
  public void lookupOperatorOverloads(
      final SqlIdentifier opName,
      final SqlFunctionCategory category,
      final SqlSyntax syntax,
      final List<SqlOperator> operatorList
  )
  {
    if (opName.names.size() == 1 && syntax == SqlSyntax.FUNCTION) {
      final SqlAggregator aggregator = aggregators.get(StringUtils.toLowerCase(opName.getSimple()));
      if (aggregator != null) {
        operatorList.add(aggregator.calciteFunction());
      }

      final SqlOperatorConversion extractionFunction = extractionOperators.get(StringUtils.toLowerCase(opName.getSimple()));
      if (extractionFunction != null) {
        operatorList.add(extractionFunction.calciteOperator());
      }
    }

    STANDARD_TABLE.lookupOperatorOverloads(opName, category, syntax, operatorList);
  }

  @Override
  public List<SqlOperator> getOperatorList()
  {
    final List<SqlOperator> retVal = new ArrayList<>();
    for (SqlAggregator aggregator : aggregators.values()) {
      retVal.add(aggregator.calciteFunction());
    }
    for (SqlOperatorConversion extractionFunction : extractionOperators.values()) {
      retVal.add(extractionFunction.calciteOperator());
    }
    retVal.addAll(STANDARD_TABLE.getOperatorList());
    return retVal;
  }
}
