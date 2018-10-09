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

package org.apache.druid.java.util.common.parsers;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.ISODateTimeFormat;

import java.util.concurrent.TimeUnit;

public class TimestampParser
{
  public static Function<String, DateTime> createTimestampParser(
      final String format
  )
  {
    if ("auto".equalsIgnoreCase(format)) {
      // Could be iso or millis
      final DateTimes.UtcFormatter parser = DateTimes.wrapFormatter(createAutoParser());
      return (String input) -> {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(input), "null timestamp");

        for (int i = 0; i < input.length(); i++) {
          if (input.charAt(i) < '0' || input.charAt(i) > '9') {
            input = ParserUtils.stripQuotes(input);
            int lastIndex = input.lastIndexOf(' ');
            DateTimeZone timeZone = DateTimeZone.UTC;
            if (lastIndex > 0) {
              DateTimeZone timeZoneFromString = ParserUtils.getDateTimeZone(input.substring(lastIndex + 1));
              if (timeZoneFromString != null) {
                timeZone = timeZoneFromString;
                input = input.substring(0, lastIndex);
              }
            }

            return parser.parse(input).withZone(timeZone);
          }
        }

        return DateTimes.utc(Long.parseLong(input));
      };
    } else if ("iso".equalsIgnoreCase(format)) {
      return input -> {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(input), "null timestamp");
        return DateTimes.of(ParserUtils.stripQuotes(input));
      };
    } else if ("posix".equalsIgnoreCase(format)
               || "millis".equalsIgnoreCase(format)
               || "micro".equalsIgnoreCase(format)
               || "nano".equalsIgnoreCase(format)) {
      final Function<Number, DateTime> numericFun = createNumericTimestampParser(format);
      return input -> {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(input), "null timestamp");
        return numericFun.apply(Long.parseLong(ParserUtils.stripQuotes(input)));
      };
    } else if ("ruby".equalsIgnoreCase(format)) {
      final Function<Number, DateTime> numericFun = createNumericTimestampParser(format);
      return input -> {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(input), "null timestamp");
        return numericFun.apply(Double.parseDouble(ParserUtils.stripQuotes(input)));
      };
    } else {
      try {
        final DateTimes.UtcFormatter formatter = DateTimes.wrapFormatter(DateTimeFormat.forPattern(format));
        return input -> {
          Preconditions.checkArgument(!Strings.isNullOrEmpty(input), "null timestamp");
          return formatter.parse(ParserUtils.stripQuotes(input));
        };
      }
      catch (Exception e) {
        throw new IAE(e, "Unable to parse timestamps with format [%s]", format);
      }
    }
  }

  public static Function<Number, DateTime> createNumericTimestampParser(
      final String format
  )
  {
    if ("posix".equalsIgnoreCase(format)) {
      return input -> DateTimes.utc(TimeUnit.SECONDS.toMillis(input.longValue()));
    } else if ("micro".equalsIgnoreCase(format)) {
      return input -> DateTimes.utc(TimeUnit.MICROSECONDS.toMillis(input.longValue()));
    } else if ("nano".equalsIgnoreCase(format)) {
      return input -> DateTimes.utc(TimeUnit.NANOSECONDS.toMillis(input.longValue()));
    } else if ("ruby".equalsIgnoreCase(format)) {
      return input -> DateTimes.utc(Double.valueOf(input.doubleValue() * 1000).longValue());
    } else {
      return input -> DateTimes.utc(input.longValue());
    }
  }

  public static Function<Object, DateTime> createObjectTimestampParser(
      final String format
  )
  {
    final Function<String, DateTime> stringFun = createTimestampParser(format);
    final Function<Number, DateTime> numericFun = createNumericTimestampParser(format);

    return o -> {
      Preconditions.checkNotNull(o, "null timestamp");

      if (o instanceof Number) {
        return numericFun.apply((Number) o);
      } else {
        return stringFun.apply(o.toString());
      }
    };
  }

  private static DateTimeFormatter createAutoParser()
  {
    final DateTimeFormatter offsetElement = new DateTimeFormatterBuilder()
        .appendTimeZoneOffset("Z", true, 2, 4)
        .toFormatter();

    DateTimeParser timeOrOffset = new DateTimeFormatterBuilder()
        .append(
            null,
            new DateTimeParser[]{
                new DateTimeFormatterBuilder().appendLiteral('T').toParser(),
                new DateTimeFormatterBuilder().appendLiteral(' ').toParser()
            }
        )
        .appendOptional(ISODateTimeFormat.timeElementParser().getParser())
        .appendOptional(offsetElement.getParser())
        .toParser();

    return new DateTimeFormatterBuilder()
        .append(ISODateTimeFormat.dateElementParser())
        .appendOptional(timeOrOffset)
        .toFormatter();
  }
}
