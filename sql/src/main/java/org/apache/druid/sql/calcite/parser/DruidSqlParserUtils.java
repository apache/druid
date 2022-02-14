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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.sql.calcite.expression.TimeUnits;
import org.apache.druid.sql.calcite.expression.builtin.TimeFloorOperatorConversion;
import org.joda.time.Period;

import java.util.List;

public class DruidSqlParserUtils
{

  private static final Logger log = new Logger(DruidSqlParserUtils.class);

  /**
   * Delegates to {@code convertSqlNodeToGranularity} and converts the exceptions to {@link ParseException}
   * with the underlying message
   */
  public static Granularity convertSqlNodeToGranularityThrowingParseExceptions(SqlNode sqlNode) throws ParseException
  {
    try {
      return convertSqlNodeToGranularity(sqlNode);
    }
    catch (Exception e) {
      log.debug(e, StringUtils.format("Unable to convert %s to a valid granularity.", sqlNode.toString()));
      throw new ParseException(e.getMessage());
    }
  }

  /**
   * This method is used to extract the granularity from a SqlNode representing following function calls:
   * 1. FLOOR(__time TO TimeUnit)
   * 2. TIME_FLOOR(__time, 'PT1H')
   *
   * Validation on the sqlNode is contingent to following conditions:
   * 1. sqlNode is an instance of SqlCall
   * 2. Operator is either one of TIME_FLOOR or FLOOR
   * 3. Number of operands in the call are 2
   * 4. First operand is a SimpleIdentifier representing __time
   * 5. If operator is TIME_FLOOR, the second argument is a literal, and can be converted to the Granularity class
   * 6. If operator is FLOOR, the second argument is a TimeUnit, and can be mapped using {@link TimeUnits}
   *
   * Since it is to be used primarily while parsing the SqlNode, it is wrapped in {@code convertSqlNodeToGranularityThrowingParseExceptions}
   *
   * @param sqlNode SqlNode representing a call to a function
   * @return Granularity as intended by the function call
   * @throws ParseException SqlNode cannot be converted a granularity
   */
  public static Granularity convertSqlNodeToGranularity(SqlNode sqlNode) throws ParseException
  {

    final String genericParseFailedMessageFormatString = "Encountered %s after PARTITIONED BY. "
                                                         + "Expected HOUR, DAY, MONTH, YEAR, ALL TIME, FLOOR function or %s function";

    if (!(sqlNode instanceof SqlCall)) {
      throw new ParseException(StringUtils.format(
          genericParseFailedMessageFormatString,
          sqlNode.toString(),
          TimeFloorOperatorConversion.SQL_FUNCTION_NAME
      ));
    }
    SqlCall sqlCall = (SqlCall) sqlNode;

    String operatorName = sqlCall.getOperator().getName();

    Preconditions.checkArgument(
        "FLOOR".equalsIgnoreCase(operatorName)
        || TimeFloorOperatorConversion.SQL_FUNCTION_NAME.equalsIgnoreCase(operatorName),
        StringUtils.format(
            "PARTITIONED BY clause only supports FLOOR(__time TO <unit> and %s(__time, period) functions",
            TimeFloorOperatorConversion.SQL_FUNCTION_NAME
        )
    );

    List<SqlNode> operandList = sqlCall.getOperandList();
    Preconditions.checkArgument(
        operandList.size() == 2,
        StringUtils.format("%s in PARTITIONED BY clause must have two arguments", operatorName)
    );


    // Check if the first argument passed in the floor function is __time
    SqlNode timeOperandSqlNode = operandList.get(0);
    Preconditions.checkArgument(
        timeOperandSqlNode.getKind().equals(SqlKind.IDENTIFIER),
        StringUtils.format("First argument to %s in PARTITIONED BY clause can only be __time", operatorName)
    );
    SqlIdentifier timeOperandSqlIdentifier = (SqlIdentifier) timeOperandSqlNode;
    Preconditions.checkArgument(
        timeOperandSqlIdentifier.getSimple().equals(ColumnHolder.TIME_COLUMN_NAME),
        StringUtils.format("First argument to %s in PARTITIONED BY clause can only be __time", operatorName)
    );

    // If the floor function is of form TIME_FLOOR(__time, 'PT1H')
    if (operatorName.equalsIgnoreCase(TimeFloorOperatorConversion.SQL_FUNCTION_NAME)) {
      SqlNode granularitySqlNode = operandList.get(1);
      Preconditions.checkArgument(
          granularitySqlNode.getKind().equals(SqlKind.LITERAL),
          "Second argument to TIME_FLOOR in PARTITIONED BY clause must be a period like 'PT1H'"
      );
      String granularityString = SqlLiteral.unchain(granularitySqlNode).toValue();
      Period period;
      try {
        period = new Period(granularityString);
      }
      catch (IllegalArgumentException e) {
        throw new ParseException(StringUtils.format("%s is an invalid period string", granularitySqlNode.toString()));
      }
      return new PeriodGranularity(period, null, null);

    } else if ("FLOOR".equalsIgnoreCase(operatorName)) { // If the floor function is of form FLOOR(__time TO DAY)
      SqlNode granularitySqlNode = operandList.get(1);
      // In future versions of Calcite, this can be checked via
      // granularitySqlNode.getKind().equals(SqlKind.INTERVAL_QUALIFIER)
      Preconditions.checkArgument(
          granularitySqlNode instanceof SqlIntervalQualifier,
          "Second argument to the FLOOR function in PARTITIONED BY clause is not a valid granularity. "
          + "Please refer to the documentation of FLOOR function"
      );
      SqlIntervalQualifier granularityIntervalQualifier = (SqlIntervalQualifier) granularitySqlNode;

      Period period = TimeUnits.toPeriod(granularityIntervalQualifier.timeUnitRange);
      Preconditions.checkNotNull(
          period,
          StringUtils.format(
              "%s is not a valid granularity for ingestion",
              granularityIntervalQualifier.timeUnitRange.toString()
          )
      );
      return new PeriodGranularity(period, null, null);
    }

    // Shouldn't reach here
    throw new ParseException(StringUtils.format(
        genericParseFailedMessageFormatString,
        sqlNode.toString(),
        TimeFloorOperatorConversion.SQL_FUNCTION_NAME
    ));
  }
}
