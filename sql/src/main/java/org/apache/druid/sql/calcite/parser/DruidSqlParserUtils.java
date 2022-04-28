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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.sql.calcite.expression.TimeUnits;
import org.apache.druid.sql.calcite.expression.builtin.TimeFloorOperatorConversion;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.filtration.MoveTimeFiltersToIntervals;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.base.AbstractInterval;

import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

  public static List<String> validateQueryAndConvertToIntervals(
      SqlNode replaceTimeQuery,
      Granularity granularity,
      DateTimeZone dateTimeZone
  ) throws ValidationException
  {
    if (replaceTimeQuery instanceof SqlLiteral && "all".equalsIgnoreCase(((SqlLiteral) replaceTimeQuery).toValue())) {
      return ImmutableList.of("all");
    }

    DimFilter dimFilter = convertQueryToDimFilter(replaceTimeQuery, dateTimeZone);
    if (!ImmutableSet.of(ColumnHolder.TIME_COLUMN_NAME).equals(dimFilter.getRequiredColumns())) {
      throw new ValidationException("Only " + ColumnHolder.TIME_COLUMN_NAME + " column is supported in OVERWRITE WHERE clause");
    }

    Filtration filtration = Filtration.create(dimFilter);
    filtration = MoveTimeFiltersToIntervals.instance().apply(filtration);
    List<Interval> intervals = filtration.getIntervals();

    for (Interval interval : intervals) {
      DateTime intervalStart = interval.getStart();
      DateTime intervalEnd = interval.getEnd();
      if (!granularity.bucketStart(intervalStart).equals(intervalStart) || !granularity.bucketStart(intervalEnd).equals(intervalEnd)) {
        throw new ValidationException("OVERWRITE WHERE clause contains an interval which is not aligned with PARTITIONED BY granularity");
      }
    }
    return intervals
        .stream()
        .map(AbstractInterval::toString)
        .collect(Collectors.toList());
  }

  public static DimFilter convertQueryToDimFilter(SqlNode replaceTimeQuery, DateTimeZone dateTimeZone)
      throws ValidationException
  {
    if (replaceTimeQuery instanceof SqlBasicCall) {
      SqlBasicCall sqlBasicCall = (SqlBasicCall) replaceTimeQuery;
      Pair<String, String> columnValuePair;
      switch (sqlBasicCall.getOperator().getKind()) {
        case AND:
          List<DimFilter> dimFilters = new ArrayList<>();
          for (SqlNode sqlNode : sqlBasicCall.getOperandList()) {
            dimFilters.add(convertQueryToDimFilter(sqlNode, dateTimeZone));
          }
          return new AndDimFilter(dimFilters);
        case OR:
          dimFilters = new ArrayList<>();
          for (SqlNode sqlNode : sqlBasicCall.getOperandList()) {
            dimFilters.add(convertQueryToDimFilter(sqlNode, dateTimeZone));
          }
          return new OrDimFilter(dimFilters);
        case NOT:
          return new NotDimFilter(convertQueryToDimFilter(sqlBasicCall.getOperandList().get(0), dateTimeZone));
        case GREATER_THAN_OR_EQUAL:
          columnValuePair = createColumnValuePair(sqlBasicCall.getOperandList(), dateTimeZone);
          return new BoundDimFilter(columnValuePair.left, columnValuePair.right, null, false, null, null, null, StringComparators.NUMERIC);
        case LESS_THAN_OR_EQUAL:
          columnValuePair = createColumnValuePair(sqlBasicCall.getOperandList(), dateTimeZone);
          return new BoundDimFilter(columnValuePair.left, null, columnValuePair.right, null, false, null, null, StringComparators.NUMERIC);
        case GREATER_THAN:
          columnValuePair = createColumnValuePair(sqlBasicCall.getOperandList(), dateTimeZone);
          return new BoundDimFilter(columnValuePair.left, columnValuePair.right, null, true, null, null, null, StringComparators.NUMERIC);
        case LESS_THAN:
          columnValuePair = createColumnValuePair(sqlBasicCall.getOperandList(), dateTimeZone);
          return new BoundDimFilter(columnValuePair.left, null, columnValuePair.right, null, true, null, null, StringComparators.NUMERIC);
        default:
          throw new ValidationException("Unsupported operation in OVERWRITE WHERE clause: " + sqlBasicCall.getOperator().getName());
      }
    }
    throw new ValidationException("Invalid OVERWRITE WHERE clause");
  }

  public static Pair<String, String> createColumnValuePair(List<SqlNode> operands, DateTimeZone timeZone) throws ValidationException
  {
    SqlNode columnName = operands.get(0);
    SqlNode timeLiteral = operands.get(1);
    if (!(columnName instanceof SqlIdentifier) || !(timeLiteral instanceof SqlTimestampLiteral)) {
      throw new ValidationException("Expressions must be of the form __time <operator> TIMESTAMP");
    }

    Timestamp sqlTimestamp = Timestamp.valueOf(((SqlTimestampLiteral) timeLiteral).toFormattedString());
    ZonedDateTime zonedTimestamp = sqlTimestamp.toLocalDateTime().atZone(timeZone.toTimeZone().toZoneId());
    return Pair.of(columnName.toString(), String.valueOf(zonedTimestamp.toInstant().toEpochMilli()));
  }
}
