/*
 * Copyright 2011 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.java.util.common.parsers;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class TimestampParser
{
  public static Function<String, DateTime> createTimestampParser(
      final String format
  )
  {
    if (format.equalsIgnoreCase("auto")) {
      // Could be iso or millis
      return new Function<String, DateTime>()
      {
        @Override
        public DateTime apply(String input)
        {
          Preconditions.checkArgument(input != null && !input.isEmpty(), "null timestamp");

          for (int i = 0; i < input.length(); i++) {
            if (input.charAt(i) < '0' || input.charAt(i) > '9') {
              return new DateTime(ParserUtils.stripQuotes(input));
            }
          }

          return new DateTime(Long.parseLong(input));
        }
      };
    } else if (format.equalsIgnoreCase("iso")) {
      return new Function<String, DateTime>()
      {
        @Override
        public DateTime apply(String input)
        {
          Preconditions.checkArgument(input != null && !input.isEmpty(), "null timestamp");
          return new DateTime(ParserUtils.stripQuotes(input));
        }
      };
    } else if (format.equalsIgnoreCase("posix")
               || format.equalsIgnoreCase("millis")
               || format.equalsIgnoreCase("nano")) {
      final Function<Number, DateTime> numericFun = createNumericTimestampParser(format);
      return new Function<String, DateTime>()
      {
        @Override
        public DateTime apply(String input)
        {
          Preconditions.checkArgument(input != null && !input.isEmpty(), "null timestamp");
          return numericFun.apply(Long.parseLong(ParserUtils.stripQuotes(input)));
        }
      };
    } else if (format.equalsIgnoreCase("ruby")) {
      // Numeric parser ignores millis for ruby.
      final Function<Number, DateTime> numericFun = createNumericTimestampParser(format);
      return new Function<String, DateTime>()
      {
        @Override
        public DateTime apply(String input)
        {
          Preconditions.checkArgument(input != null && !input.isEmpty(), "null timestamp");
          return numericFun.apply(Double.parseDouble(ParserUtils.stripQuotes(input)));
        }
      };
    } else {
      try {
        final DateTimeFormatter formatter = DateTimeFormat.forPattern(format);
        return new Function<String, DateTime>()
        {
          @Override
          public DateTime apply(String input)
          {
            Preconditions.checkArgument(input != null && !input.isEmpty(), "null timestamp");
            return formatter.parseDateTime(ParserUtils.stripQuotes(input));
          }
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
    // Ignore millis for ruby
    if (format.equalsIgnoreCase("posix") || format.equalsIgnoreCase("ruby")) {
      return new Function<Number, DateTime>()
      {
        @Override
        public DateTime apply(Number input)
        {
          return new DateTime(input.longValue() * 1000);
        }
      };
    } else if (format.equalsIgnoreCase("nano")) {
      return new Function<Number, DateTime>()
      {
        @Override
        public DateTime apply(Number input)
        {
          return new DateTime(input.longValue() / 1000000L);
        }
      };
    } else {
      return new Function<Number, DateTime>()
      {
        @Override
        public DateTime apply(Number input)
        {
          return new DateTime(input.longValue());
        }
      };
    }
  }

  public static Function<Object, DateTime> createObjectTimestampParser(
      final String format
  )
  {
    final Function<String, DateTime> stringFun = createTimestampParser(format);
    final Function<Number, DateTime> numericFun = createNumericTimestampParser(format);

    return new Function<Object, DateTime>()
    {
      @Override
      public DateTime apply(Object o)
      {
        Preconditions.checkArgument(o != null, "null timestamp");

        if (o instanceof Number) {
          return numericFun.apply((Number) o);
        } else {
          return stringFun.apply(o.toString());
        }
      }
    };
  }
}
