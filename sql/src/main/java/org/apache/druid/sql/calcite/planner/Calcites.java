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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Chars;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.math.expr.ExpressionProcessingConfig;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Utility functions for Calcite.
 * <p>
 * See also the file {@code saffron.properties} which holds the
 * character set system properties formerly set in this file.
 */
public class Calcites
{
  private static final DateTimes.UtcFormatter CALCITE_DATE_PARSER = DateTimes.wrapFormatter(ISODateTimeFormat.dateParser());
  private static final DateTimes.UtcFormatter CALCITE_TIMESTAMP_PARSER = DateTimes.wrapFormatter(
      new DateTimeFormatterBuilder()
          .append(ISODateTimeFormat.dateParser())
          .appendLiteral(' ')
          .append(ISODateTimeFormat.timeParser())
          .toFormatter()
  );

  private static final DateTimeFormatter CALCITE_TIME_PRINTER = DateTimeFormat.forPattern("HH:mm:ss.S");
  private static final DateTimeFormatter CALCITE_DATE_PRINTER = DateTimeFormat.forPattern("yyyy-MM-dd");
  private static final DateTimeFormatter CALCITE_TIMESTAMP_PRINTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");

  private static final Charset DEFAULT_CHARSET = Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);

  private static final Pattern TRAILING_ZEROS = Pattern.compile("\\.?0+$");

  public static final SqlReturnTypeInference
      ARG0_NULLABLE_ARRAY_RETURN_TYPE_INFERENCE = new Arg0NullableArrayTypeInference();
  public static final SqlReturnTypeInference
      ARG1_NULLABLE_ARRAY_RETURN_TYPE_INFERENCE = new Arg1NullableArrayTypeInference();

  private Calcites()
  {
    // No instantiation.
  }

  public static Charset defaultCharset()
  {
    return DEFAULT_CHARSET;
  }

  public static String escapeStringLiteral(final String s)
  {
    Preconditions.checkNotNull(s);
    boolean isPlainAscii = true;
    final StringBuilder builder = new StringBuilder("'");
    for (int i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      if (Character.isLetterOrDigit(c) || c == ' ') {
        builder.append(c);
        if (c > 127) {
          isPlainAscii = false;
        }
      } else {
        builder.append("\\").append(BaseEncoding.base16().encode(Chars.toByteArray(c)));
        isPlainAscii = false;
      }
    }
    builder.append("'");
    return isPlainAscii ? builder.toString() : "U&" + builder;
  }

  /**
   * Returns whether an expression is a literal. Like {@link RexUtil#isLiteral(RexNode, boolean)} but can accept
   * arrays too.
   *
   * @param rexNode    expression
   * @param allowCast  allow calls to CAST if its argument is literal
   * @param allowArray allow calls to ARRAY if its arguments are literal
   */
  public static boolean isLiteral(RexNode rexNode, boolean allowCast, boolean allowArray)
  {
    if (RexUtil.isLiteral(rexNode, allowCast)) {
      return true;
    } else if (allowArray && rexNode.isA(SqlKind.ARRAY_VALUE_CONSTRUCTOR)) {
      for (final RexNode element : ((RexCall) rexNode).getOperands()) {
        if (!RexUtil.isLiteral(element, allowCast)) {
          return false;
        }
      }

      return true;
    } else {
      return false;
    }
  }

  /**
   * Convert {@link RelDataType} to the most appropriate {@link ColumnType}, or null if there is no {@link ColumnType}
   * that is appropriate for this {@link RelDataType}.
   *
   * Equivalent to {@link #getValueTypeForRelDataTypeFull(RelDataType)}, except this method returns
   * {@link ColumnType#STRING} when {@link ColumnType#isArray()} if
   * {@link ExpressionProcessingConfig#processArraysAsMultiValueStrings()} is set via the server property
   * {@code druid.expressions.allowArrayToStringCast}.
   *
   * @return type, or null if there is no matching type
   */
  @Nullable
  public static ColumnType getColumnTypeForRelDataType(final RelDataType type)
  {
    ColumnType valueType = getValueTypeForRelDataTypeFull(type);
    // coerce array to multi value string
    if (ExpressionProcessing.processArraysAsMultiValueStrings() && valueType != null && valueType.isArray()) {
      return ColumnType.STRING;
    }
    return valueType;
  }

  /**
   * Convert {@link RelDataType} to the most appropriate {@link ColumnType}, or null if there is no {@link ColumnType}
   * that is appropriate for this {@link RelDataType}.
   *
   * Equivalent to {@link #getColumnTypeForRelDataType(RelDataType)}, but ignores
   * {@link ExpressionProcessingConfig#processArraysAsMultiValueStrings()} (and acts as if it is false).
   *
   * @return type, or null if there is no matching type
   */
  @Nullable
  public static ColumnType getValueTypeForRelDataTypeFull(final RelDataType type)
  {
    final SqlTypeName sqlTypeName = type.getSqlTypeName();
    if (SqlTypeName.FLOAT == sqlTypeName) {
      return ColumnType.FLOAT;
    } else if (isDoubleType(sqlTypeName)) {
      return ColumnType.DOUBLE;
    } else if (isLongType(sqlTypeName)) {
      return ColumnType.LONG;
    } else if (isStringType(sqlTypeName)) {
      return ColumnType.STRING;
    } else if (SqlTypeName.OTHER == sqlTypeName) {
      if (type instanceof RowSignatures.ComplexSqlType) {
        return ColumnType.ofComplex(((RowSignatures.ComplexSqlType) type).getComplexTypeName());
      }
      return ColumnType.UNKNOWN_COMPLEX;
    } else if (sqlTypeName == SqlTypeName.ARRAY) {
      ColumnType elementType = getValueTypeForRelDataTypeFull(type.getComponentType());
      if (elementType != null) {
        return ColumnType.ofArray(elementType);
      }
      return null;
    } else {
      return null;
    }
  }

  public static boolean isStringType(SqlTypeName sqlTypeName)
  {
    return SqlTypeName.CHAR_TYPES.contains(sqlTypeName) ||
           SqlTypeName.INTERVAL_TYPES.contains(sqlTypeName);
  }

  public static boolean isDoubleType(SqlTypeName sqlTypeName)
  {
    return SqlTypeName.FRACTIONAL_TYPES.contains(sqlTypeName) || SqlTypeName.APPROX_TYPES.contains(sqlTypeName);
  }

  public static boolean isLongType(SqlTypeName sqlTypeName)
  {
    return SqlTypeName.TIMESTAMP == sqlTypeName ||
           SqlTypeName.DATE == sqlTypeName ||
           SqlTypeName.BOOLEAN == sqlTypeName ||
           SqlTypeName.INT_TYPES.contains(sqlTypeName);
  }

  /**
   * Returns the natural StringComparator associated with the RelDataType
   */
  public static StringComparator getStringComparatorForRelDataType(RelDataType dataType)
  {
    final ColumnType valueType = getColumnTypeForRelDataType(dataType);
    return getStringComparatorForValueType(valueType);
  }

  /**
   * Returns the natural StringComparator associated with the given ColumnType
   */
  public static StringComparator getStringComparatorForValueType(ColumnType valueType)
  {
    if (valueType.isNumeric()) {
      return StringComparators.NUMERIC;
    } else if (valueType.is(ValueType.STRING)) {
      return StringComparators.LEXICOGRAPHIC;
    } else {
      return StringComparators.NATURAL;
    }
  }

  /**
   * Like RelDataTypeFactory.createSqlType, but creates types that align best with how Druid represents them.
   */
  public static RelDataType createSqlType(final RelDataTypeFactory typeFactory, final SqlTypeName typeName)
  {
    return createSqlTypeWithNullability(typeFactory, typeName, false);
  }

  /**
   * Like RelDataTypeFactory.createSqlTypeWithNullability, but creates types that align best with how Druid
   * represents them.
   */
  public static RelDataType createSqlTypeWithNullability(
      final RelDataTypeFactory typeFactory,
      final SqlTypeName typeName,
      final boolean nullable
  )
  {
    final RelDataType dataType;

    switch (typeName) {
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        // Our timestamps are down to the millisecond (precision = 3).
        dataType = typeFactory.createSqlType(typeName, DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION);
        break;
      case CHAR:
      case VARCHAR:
        dataType = typeFactory.createTypeWithCharsetAndCollation(
            typeFactory.createSqlType(typeName),
            Calcites.defaultCharset(),
            SqlCollation.IMPLICIT
        );
        break;
      default:
        dataType = typeFactory.createSqlType(typeName);
    }
    return typeFactory.createTypeWithNullability(dataType, nullable);
  }

  /**
   * Like RelDataTypeFactory.createSqlTypeWithNullability, but creates types that align best with how Druid
   * represents them.
   */
  public static RelDataType createSqlArrayTypeWithNullability(
      final RelDataTypeFactory typeFactory,
      final SqlTypeName elementTypeName,
      final boolean nullable
  )
  {
    final RelDataType dataType = typeFactory.createTypeWithNullability(
        typeFactory.createArrayType(
            createSqlTypeWithNullability(typeFactory, elementTypeName, nullable),
            -1
        ),
        true
    );

    return dataType;
  }

  /**
   * Calcite expects "TIMESTAMP" types to be an instant that has the expected local time fields if printed as UTC.
   *
   * @param dateTime joda timestamp
   * @param timeZone session time zone
   *
   * @return Calcite style millis
   */
  public static long jodaToCalciteTimestamp(final DateTime dateTime, final DateTimeZone timeZone)
  {
    return dateTime.withZone(timeZone).withZoneRetainFields(DateTimeZone.UTC).getMillis();
  }

  /**
   * Calcite expects "DATE" types to be number of days from the epoch to the UTC date matching the local time fields.
   *
   * @param dateTime joda timestamp
   * @param timeZone session time zone
   *
   * @return Calcite style date
   */
  public static int jodaToCalciteDate(final DateTime dateTime, final DateTimeZone timeZone)
  {
    final DateTime date = dateTime.withZone(timeZone).dayOfMonth().roundFloorCopy();
    return Days.daysBetween(DateTimes.EPOCH, date.withZoneRetainFields(DateTimeZone.UTC)).getDays();
  }

  /**
   * Creates a Calcite TIMESTAMP literal from a Joda DateTime.
   *
   * @param dateTime        joda timestamp
   * @param sessionTimeZone session time zone
   *
   * @return Calcite style Calendar, appropriate for literals
   */
  public static RexLiteral jodaToCalciteTimestampLiteral(
      final RexBuilder rexBuilder,
      final DateTime dateTime,
      final DateTimeZone sessionTimeZone,
      final int precision
  )
  {
    return rexBuilder.makeTimestampLiteral(jodaToCalciteTimestampString(dateTime, sessionTimeZone), precision);
  }

  /**
   * Calcite expects TIMESTAMP literals to be represented by TimestampStrings in the local time zone.
   *
   * @param dateTime        joda timestamp
   * @param sessionTimeZone session time zone
   *
   * @return Calcite style Calendar, appropriate for literals
   */
  public static TimestampString jodaToCalciteTimestampString(
      final DateTime dateTime,
      final DateTimeZone sessionTimeZone
  )
  {
    // Calcite expects TIMESTAMP literals to be represented by TimestampStrings in the session time zone.
    // The TRAILING_ZEROS ... replaceAll is because Calcite doesn't like trailing zeroes in its fractional seconds part.
    final String timestampString = TRAILING_ZEROS
        .matcher(CALCITE_TIMESTAMP_PRINTER.print(dateTime.withZone(sessionTimeZone)))
        .replaceAll("");

    return new TimestampString(timestampString);
  }

  /**
   * Calcite expects TIME literals to be represented by TimeStrings in the local time zone.
   *
   * @param dateTime joda timestamp
   * @param timeZone session time zone
   *
   * @return Calcite style Calendar, appropriate for literals
   */
  public static TimeString jodaToCalciteTimeString(final DateTime dateTime, final DateTimeZone timeZone)
  {
    // The replaceAll is because Calcite doesn't like trailing zeroes in its fractional seconds part.
    String timeString = TRAILING_ZEROS
        .matcher(CALCITE_TIME_PRINTER.print(dateTime.withZone(timeZone)))
        .replaceAll("");
    return new TimeString(timeString);
  }

  /**
   * Calcite expects DATE literals to be represented by DateStrings in the local time zone.
   *
   * @param dateTime joda timestamp
   * @param timeZone session time zone
   *
   * @return Calcite style Calendar, appropriate for literals
   */
  public static DateString jodaToCalciteDateString(final DateTime dateTime, final DateTimeZone timeZone)
  {
    return new DateString(CALCITE_DATE_PRINTER.print(dateTime.withZone(timeZone)));
  }

  /**
   * Translates "literal" (a TIMESTAMP or DATE literal) to milliseconds since the epoch using the provided
   * session time zone.
   *
   * @param literal  TIMESTAMP or DATE literal
   * @param timeZone session time zone
   *
   * @return milliseconds time
   */
  public static DateTime calciteDateTimeLiteralToJoda(final RexNode literal, final DateTimeZone timeZone)
  {
    final SqlTypeName typeName = literal.getType().getSqlTypeName();
    if (literal.getKind() != SqlKind.LITERAL || (typeName != SqlTypeName.TIMESTAMP && typeName != SqlTypeName.DATE)) {
      throw new IAE("Expected literal but got[%s]", literal.getKind());
    }

    if (typeName == SqlTypeName.TIMESTAMP) {
      final TimestampString timestampString = (TimestampString) RexLiteral.value(literal);
      return CALCITE_TIMESTAMP_PARSER.parse(timestampString.toString()).withZoneRetainFields(timeZone);
    } else if (typeName == SqlTypeName.DATE) {
      final DateString dateString = (DateString) RexLiteral.value(literal);
      return CALCITE_DATE_PARSER.parse(dateString.toString()).withZoneRetainFields(timeZone);
    } else {
      throw new IAE("Expected TIMESTAMP or DATE but got[%s]", typeName);
    }
  }

  /**
   * The inverse of {@link #jodaToCalciteTimestamp(DateTime, DateTimeZone)}.
   *
   * @param timestamp Calcite style timestamp
   * @param timeZone  session time zone
   *
   * @return joda timestamp, with time zone set to the session time zone
   */
  public static DateTime calciteTimestampToJoda(final long timestamp, final DateTimeZone timeZone)
  {
    return new DateTime(timestamp, DateTimeZone.UTC).withZoneRetainFields(timeZone);
  }

  /**
   * The inverse of {@link #jodaToCalciteDate(DateTime, DateTimeZone)}.
   *
   * @param date     Calcite style date
   * @param timeZone session time zone
   *
   * @return joda timestamp, with time zone set to the session time zone
   */
  public static DateTime calciteDateToJoda(final int date, final DateTimeZone timeZone)
  {
    return DateTimes.EPOCH.plusDays(date).withZoneRetainFields(timeZone);
  }

  /**
   * Find a string that is either equal to "basePrefix", or basePrefix prepended by underscores, and where nothing in
   * "strings" starts with prefix plus a digit.
   */
  public static String findUnusedPrefixForDigits(final String basePrefix, final Iterable<String> strings)
  {
    final NavigableSet<String> navigableStrings;

    if (strings instanceof NavigableSet) {
      navigableStrings = (NavigableSet<String>) strings;
    } else {
      navigableStrings = new TreeSet<>();
      Iterables.addAll(navigableStrings, strings);
    }

    String prefix = basePrefix;

    while (!isUnusedPrefix(prefix, navigableStrings)) {
      prefix = "_" + prefix;
    }

    return prefix;
  }

  private static boolean isUnusedPrefix(final String prefix, final NavigableSet<String> strings)
  {
    // ":" is one character after "9"
    final NavigableSet<String> subSet = strings.subSet(prefix + "0", true, prefix + ":", false);
    return subSet.isEmpty();
  }

  public static String makePrefixedName(final String prefix, final String suffix)
  {
    return StringUtils.format("%s:%s", prefix, suffix);
  }

  public static Class<?> sqlTypeNameJdbcToJavaClass(SqlTypeName typeName)
  {
    // reference: https://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
    JDBCType jdbcType = JDBCType.valueOf(typeName.getJdbcOrdinal());
    switch (jdbcType) {
      case CHAR:
      case VARCHAR:
      case LONGVARCHAR:
        return String.class;
      case NUMERIC:
      case DECIMAL:
        return BigDecimal.class;
      case BIT:
        return Boolean.class;
      case TINYINT:
        return Byte.class;
      case SMALLINT:
        return Short.class;
      case INTEGER:
        return Integer.class;
      case BIGINT:
        return Long.class;
      case REAL:
        return Float.class;
      case FLOAT:
      case DOUBLE:
        return Double.class;
      case BINARY:
      case VARBINARY:
        return Byte[].class;
      case DATE:
        return Date.class;
      case TIME:
        return Time.class;
      case TIMESTAMP:
        return Timestamp.class;
      default:
        return Object.class;
    }
  }

  public static class Arg0NullableArrayTypeInference implements SqlReturnTypeInference
  {
    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding)
    {
      RelDataType type = opBinding.getOperandType(0);
      if (SqlTypeUtil.isArray(type)) {
        return type;
      }
      return Calcites.createSqlArrayTypeWithNullability(
          opBinding.getTypeFactory(),
          type.getSqlTypeName(),
          true
      );
    }
  }

  public static class Arg1NullableArrayTypeInference implements SqlReturnTypeInference
  {
    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding)
    {
      RelDataType type = opBinding.getOperandType(1);
      if (SqlTypeUtil.isArray(type)) {
        return type;
      }
      return Calcites.createSqlArrayTypeWithNullability(
          opBinding.getTypeFactory(),
          type.getSqlTypeName(),
          true
      );
    }
  }
}
