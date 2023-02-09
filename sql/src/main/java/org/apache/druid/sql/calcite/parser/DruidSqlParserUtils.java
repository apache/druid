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
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DruidSqlParserUtils
{

  private static final Logger log = new Logger(DruidSqlParserUtils.class);
  public static final String ALL = "all";

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

  /**
   * This method validates and converts a {@link SqlNode} representing a query into an optmizied list of intervals to
   * be used in creating an ingestion spec. If the sqlNode is an SqlLiteral of {@link #ALL}, returns a singleton list of
   * "ALL". Otherwise, it converts and optimizes the query using {@link MoveTimeFiltersToIntervals} into a list of
   * intervals which contain all valid values of time as per the query.
   *
   * The following validations are performed
   * 1. Only __time column and timestamp literals are present in the query
   * 2. The interval after optimization is not empty
   * 3. The operands in the expression are supported
   * 4. The intervals after adjusting for timezone are aligned with the granularity parameter
   *
   * @param replaceTimeQuery Sql node representing the query
   * @param granularity granularity of the query for validation
   * @param dateTimeZone timezone
   * @return List of string representation of intervals
   * @throws ValidationException if the SqlNode cannot be converted to a list of intervals
   */
  public static List<String> validateQueryAndConvertToIntervals(
      SqlNode replaceTimeQuery,
      Granularity granularity,
      DateTimeZone dateTimeZone
  ) throws ValidationException
  {
    if (replaceTimeQuery instanceof SqlLiteral && ALL.equalsIgnoreCase(((SqlLiteral) replaceTimeQuery).toValue())) {
      return ImmutableList.of(ALL);
    }

    DimFilter dimFilter = convertQueryToDimFilter(replaceTimeQuery, dateTimeZone);

    Filtration filtration = Filtration.create(dimFilter);
    filtration = MoveTimeFiltersToIntervals.instance().apply(filtration);
    List<Interval> intervals = filtration.getIntervals();

    if (filtration.getDimFilter() != null) {
      throw new ValidationException("Only " + ColumnHolder.TIME_COLUMN_NAME + " column is supported in OVERWRITE WHERE clause");
    }

    if (intervals.isEmpty()) {
      throw new ValidationException("Intervals for replace are empty");
    }

    for (Interval interval : intervals) {
      DateTime intervalStart = interval.getStart();
      DateTime intervalEnd = interval.getEnd();
      if (!granularity.bucketStart(intervalStart).equals(intervalStart) || !granularity.bucketStart(intervalEnd).equals(intervalEnd)) {
        throw new ValidationException("OVERWRITE WHERE clause contains an interval " + intervals +
                                      " which is not aligned with PARTITIONED BY granularity " + granularity);
      }
    }
    return intervals
        .stream()
        .map(AbstractInterval::toString)
        .collect(Collectors.toList());
  }

  /**
   * Extracts and converts the information in the CLUSTERED BY clause to a new SqlOrderBy node.
   *
   * @param query sql query
   * @param clusteredByList List of clustered by columns
   * @return SqlOrderBy node containing the clusteredByList information
   */
  public static SqlOrderBy convertClusterByToOrderBy(SqlNode query, SqlNodeList clusteredByList)
  {
    // If we have a CLUSTERED BY clause, extract the information in that CLUSTERED BY and create a new
    // SqlOrderBy node
    SqlNode offset = null;
    SqlNode fetch = null;

    if (query instanceof SqlOrderBy) {
      SqlOrderBy sqlOrderBy = (SqlOrderBy) query;
      // This represents the underlying query free of OFFSET, FETCH and ORDER BY clauses
      // For a sqlOrderBy.query like "SELECT dim1, sum(dim2) FROM foo OFFSET 10 FETCH 30 ORDER BY dim1 GROUP
      // BY dim1 this would represent the "SELECT dim1, sum(dim2) from foo GROUP BY dim1
      query = sqlOrderBy.query;
      offset = sqlOrderBy.offset;
      fetch = sqlOrderBy.fetch;
    }
    // Creates a new SqlOrderBy query, which may have our CLUSTERED BY overwritten
    return new SqlOrderBy(
        query.getParserPosition(),
        query,
        clusteredByList,
        offset,
        fetch
    );
  }

  /**
   * This method is used to convert an {@link SqlNode} representing a query into a {@link DimFilter} for the same query.
   * It takes the timezone as a separate parameter, as Sql timestamps don't contain that information. Supported functions
   * are AND, OR, NOT, >, <, >=, <= and BETWEEN operators in the sql query.
   *
   * @param replaceTimeQuery Sql node representing the query
   * @param dateTimeZone timezone
   * @return Dimfilter for the query
   * @throws ValidationException if the SqlNode cannot be converted a Dimfilter
   */
  public static DimFilter convertQueryToDimFilter(SqlNode replaceTimeQuery, DateTimeZone dateTimeZone)
      throws ValidationException
  {
    if (!(replaceTimeQuery instanceof SqlBasicCall)) {
      log.error("Expected SqlBasicCall during parsing, but found " + replaceTimeQuery.getClass().getName());
      throw new ValidationException("Invalid OVERWRITE WHERE clause");
    }
    String columnName;
    SqlBasicCall sqlBasicCall = (SqlBasicCall) replaceTimeQuery;
    List<SqlNode> operandList = sqlBasicCall.getOperandList();
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
        columnName = parseColumnName(operandList.get(0));
        return new BoundDimFilter(
            columnName,
            parseTimeStampWithTimeZone(operandList.get(1), dateTimeZone),
            null,
            false,
            null,
            null,
            null,
            StringComparators.NUMERIC
        );
      case LESS_THAN_OR_EQUAL:
        columnName = parseColumnName(operandList.get(0));
        return new BoundDimFilter(
            columnName,
            null,
            parseTimeStampWithTimeZone(operandList.get(1), dateTimeZone),
            null,
            false,
            null,
            null,
            StringComparators.NUMERIC
        );
      case GREATER_THAN:
        columnName = parseColumnName(operandList.get(0));
        return new BoundDimFilter(
            columnName,
            parseTimeStampWithTimeZone(operandList.get(1), dateTimeZone),
            null,
            true,
            null,
            null,
            null,
            StringComparators.NUMERIC
        );
      case LESS_THAN:
        columnName = parseColumnName(operandList.get(0));
        return new BoundDimFilter(
            columnName,
            null,
            parseTimeStampWithTimeZone(operandList.get(1), dateTimeZone),
            null,
            true,
            null,
            null,
            StringComparators.NUMERIC
        );
      case BETWEEN:
        columnName = parseColumnName(operandList.get(0));
        return new BoundDimFilter(
            columnName,
            parseTimeStampWithTimeZone(operandList.get(1), dateTimeZone),
            parseTimeStampWithTimeZone(operandList.get(2), dateTimeZone),
            false,
            false,
            null,
            null,
            StringComparators.NUMERIC
        );
      default:
        throw new ValidationException("Unsupported operation in OVERWRITE WHERE clause: " + sqlBasicCall.getOperator().getName());
    }
  }

  /**
   * Converts a {@link SqlNode} identifier into a string representation
   *
   * @param sqlNode the sql node
   * @return string representing the column name
   * @throws ValidationException if the sql node is not an SqlIdentifier
   */
  public static String parseColumnName(SqlNode sqlNode) throws ValidationException
  {
    if (!(sqlNode instanceof SqlIdentifier)) {
      throw new ValidationException("Expressions must be of the form __time <operator> TIMESTAMP");
    }
    return ((SqlIdentifier) sqlNode).getSimple();
  }

  /**
   * Converts a {@link SqlNode} into a timestamp, taking into account the timezone
   *
   * @param sqlNode the sql node
   * @param timeZone timezone
   * @return the timestamp string as milliseconds from epoch
   * @throws ValidationException if the sql node is not a SqlTimestampLiteral
   */
  public static String parseTimeStampWithTimeZone(SqlNode sqlNode, DateTimeZone timeZone) throws ValidationException
  {
    if (!(sqlNode instanceof SqlTimestampLiteral)) {
      throw new ValidationException("Expressions must be of the form __time <operator> TIMESTAMP");
    }

    Timestamp sqlTimestamp = Timestamp.valueOf(((SqlTimestampLiteral) sqlNode).toFormattedString());
    ZonedDateTime zonedTimestamp = sqlTimestamp.toLocalDateTime().atZone(timeZone.toTimeZone().toZoneId());
    return String.valueOf(zonedTimestamp.toInstant().toEpochMilli());
  }

  /**
   * Throws an IAE with appropriate message if the granularity supplied is not present in
   * {@link org.apache.druid.java.util.common.granularity.Granularities}. It also filters out NONE as it is not a valid
   * granularity that can be supplied in PARTITIONED BY
   */
  public static void throwIfUnsupportedGranularityInPartitionedBy(Granularity granularity)
  {
    if (!GranularityType.isStandard(granularity)) {
      throw new IAE(
          "The granularity specified in PARTITIONED BY is not supported. "
          + "Please use an equivalent of these granularities: %s.",
          Arrays.stream(GranularityType.values())
                .filter(granularityType -> !granularityType.equals(GranularityType.NONE))
                .map(Enum::name)
                .map(StringUtils::toLowerCase)
                .collect(Collectors.joining(", "))
      );
    }
  }
}
