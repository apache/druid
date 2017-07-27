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

import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Chars;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.column.ValueType;
import io.druid.sql.calcite.schema.DruidSchema;
import io.druid.sql.calcite.schema.InformationSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;

import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.Locale;
import java.util.NavigableSet;
import java.util.TimeZone;
import java.util.TreeSet;

/**
 * Utility functions for Calcite.
 */
public class Calcites
{
  private static final TimeZone GMT_TIME_ZONE = TimeZone.getTimeZone("GMT");
  private static final Charset DEFAULT_CHARSET = Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);

  private Calcites()
  {
    // No instantiation.
  }

  public static void setSystemProperties()
  {
    // These properties control the charsets used for SQL literals. I don't see a way to change this except through
    // system properties, so we'll have to set those...

    final String charset = ConversionUtil.NATIVE_UTF16_CHARSET_NAME;
    System.setProperty("saffron.default.charset", Calcites.defaultCharset().name());
    System.setProperty("saffron.default.nationalcharset", Calcites.defaultCharset().name());
    System.setProperty("saffron.default.collation.name", StringUtils.format("%s$en_US", charset));
  }

  public static Charset defaultCharset()
  {
    return DEFAULT_CHARSET;
  }

  public static SchemaPlus createRootSchema(final Schema druidSchema)
  {
    final SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();
    rootSchema.add(DruidSchema.NAME, druidSchema);
    rootSchema.add(InformationSchema.NAME, new InformationSchema(rootSchema));
    return rootSchema;
  }

  public static String escapeStringLiteral(final String s)
  {
    if (s == null) {
      return "''";
    } else {
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
      return isPlainAscii ? builder.toString() : "U&" + builder.toString();
    }
  }

  public static ValueType getValueTypeForSqlTypeName(SqlTypeName sqlTypeName)
  {
    if (SqlTypeName.FLOAT == sqlTypeName) {
      return ValueType.FLOAT;
    } else if (SqlTypeName.FRACTIONAL_TYPES.contains(sqlTypeName)) {
      return ValueType.DOUBLE;
    } else if (SqlTypeName.TIMESTAMP == sqlTypeName
               || SqlTypeName.DATE == sqlTypeName
               || SqlTypeName.BOOLEAN == sqlTypeName
               || SqlTypeName.INT_TYPES.contains(sqlTypeName)) {
      return ValueType.LONG;
    } else if (SqlTypeName.CHAR_TYPES.contains(sqlTypeName)) {
      return ValueType.STRING;
    } else if (SqlTypeName.OTHER == sqlTypeName) {
      return ValueType.COMPLEX;
    } else {
      return null;
    }
  }

  public static StringComparator getStringComparatorForSqlTypeName(SqlTypeName sqlTypeName)
  {
    final ValueType valueType = getValueTypeForSqlTypeName(sqlTypeName);
    if (ValueType.isNumeric(valueType)) {
      return StringComparators.NUMERIC;
    } else if (ValueType.STRING == valueType) {
      return StringComparators.LEXICOGRAPHIC;
    } else {
      throw new ISE("Unrecognized valueType[%s]", valueType);
    }
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
    return Days.daysBetween(new DateTime(0L, DateTimeZone.UTC), date.withZoneRetainFields(DateTimeZone.UTC)).getDays();
  }

  /**
   * Calcite expects TIMESTAMP and DATE literals to be represented by Calendars that would have the expected
   * local time fields if printed as UTC.
   *
   * @param dateTime joda timestamp
   * @param timeZone session time zone
   *
   * @return Calcite style Calendar, appropriate for literals
   */
  public static Calendar jodaToCalciteCalendarLiteral(final DateTime dateTime, final DateTimeZone timeZone)
  {
    final Calendar calendar = Calendar.getInstance(Locale.ENGLISH);
    calendar.setTimeZone(GMT_TIME_ZONE);
    calendar.setTimeInMillis(Calcites.jodaToCalciteTimestamp(dateTime, timeZone));
    return calendar;
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
      throw new IAE("Expected TIMESTAMP or DATE literal but got[%s:%s]", literal.getKind(), typeName);
    }

    final Calendar calendar = (Calendar) RexLiteral.value(literal);
    return calciteTimestampToJoda(calendar.getTimeInMillis(), timeZone);
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
    return new DateTime(0L, DateTimeZone.UTC).plusDays(date).withZoneRetainFields(timeZone);
  }

  /**
   * Checks if a RexNode is a literal int or not. If this returns true, then {@code RexLiteral.intValue(literal)} can be
   * used to get the value of the literal.
   *
   * @param rexNode the node
   *
   * @return true if this is an int
   */
  public static boolean isIntLiteral(final RexNode rexNode)
  {
    return rexNode instanceof RexLiteral && SqlTypeName.INT_TYPES.contains(rexNode.getType().getSqlTypeName());
  }

  public static boolean anyStartsWith(final TreeSet<String> set, final String prefix)
  {
    final NavigableSet<String> headSet = set.headSet(prefix, true);
    return !headSet.isEmpty() && headSet.first().startsWith(prefix);
  }
}
