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

package org.apache.druid.sql.calcite.parser;

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.sql.calcite.expression.TimeUnits;
import org.apache.druid.sql.calcite.expression.builtin.TimeFloorOperatorConversion;
import org.joda.time.Period;

import java.util.List;

// TODO: More appropriate name
public class DruidSqlUtils
{
  /**
   * Delegates to {@code convertSqlNodeToGranularity} and converts the exceptions to {@link ParseException}
   * with the underlying message
   */
  public static Granularity convertSqlNodeToGranularityThrowingParseExceptions(SqlNode sqlNode) throws ParseException
  {
    Granularity g;
    try {
      g = convertSqlNodeToGranularity(sqlNode);
    }
    catch (Exception e) {
      throw new ParseException(e.getMessage());
    }
    return g;
  }


  /**
   * This method is used to extract the granularity from a SqlNode representing following function calls:
   * 1. FLOOR(__time TO TimeUnit)
   * 2. TIME_FLOOR(__time, 'PT1H')
   * <p>
   * Validation on the sqlNode is contingent to following conditions:
   * 1. sqlNode is an instance of SqlCall
   * 2. Operator is either one of TIME_FLOOR or FLOOR
   * 3. Number of operands in the call are 2
   * 4. First operand is a SimpleIdentifier representing __time
   * 5. If operator is TIME_FLOOR, the second argument is a literal, and can be converted to the Granularity class
   * 6. If operator is FLOOR, the second argument is a TimeUnit, and can be mapped using {@link org.apache.druid.sql.calcite.expression.TimeUnits}
   * <p>
   * Since it is to be used primarily while parsing the SqlNode, it is wrapped in {@code convertSqlNodeToGranularityThrowingParseExceptions}
   *
   * @param sqlNode SqlNode representing a call to a function
   * @return
   * @throws ParseException SqlNode cannot be converted a granularity
   */
  public static Granularity convertSqlNodeToGranularity(SqlNode sqlNode) throws ParseException
  {
    if (!(sqlNode instanceof SqlCall)) {
      throw new ParseException("abc");
    }
    SqlCall sqlCall = (SqlCall) sqlNode;

    List<SqlNode> operandList = sqlCall.getOperandList();
    Preconditions.checkArgument(operandList.size() == 2, "Invalid number of arguments");

    SqlOperator operator = sqlCall.getOperator();


    SqlNode timeOperandSqlNode = operandList.get(0);
    Preconditions.checkArgument(timeOperandSqlNode.getKind().equals(SqlKind.IDENTIFIER), "abc");
    SqlIdentifier timeOperandSqlIdentifier = (SqlIdentifier) timeOperandSqlNode;
    Preconditions.checkArgument(timeOperandSqlIdentifier.getSimple().equals(ColumnHolder.TIME_COLUMN_NAME), "abc");

    if (operator.getName().equals(TimeFloorOperatorConversion.SQL_FUNCTION_NAME)) {
      SqlNode granularitySqlNode = operandList.get(1);
      Preconditions.checkArgument(granularitySqlNode.getKind().equals(SqlKind.LITERAL), "abc");
      String granularityString = SqlLiteral.unchain(granularitySqlNode).toValue();
      Period period = new Period(granularityString);
      return new PeriodGranularity(period, null, null);

    } else if (operator.getName().equals("FLOOR")) {
      SqlNode granularitySqlNode = operandList.get(1);
      // In future versions of Calcite, this can be checked via
      // granularitySqlNode.getKind().equals(SqlKind.INTERVAL_QUALIFIER)
      Preconditions.checkArgument(granularitySqlNode instanceof SqlIntervalQualifier, "abc");
      SqlIntervalQualifier granularityIntervalQualifier = (SqlIntervalQualifier) granularitySqlNode;

      Period period = TimeUnits.toPeriod(granularityIntervalQualifier.timeUnitRange);
      Preconditions.checkNotNull(period, "abc");
      return new PeriodGranularity(period, null, null);
    }

    throw new ParseException("Unable to convert");
  }
}
