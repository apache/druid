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

package io.druid.segment.incremental;

import com.google.common.base.Function;
import com.metamx.common.IAE;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class TimestampFormatter
{
  public static Function<Long, String> createTimestampFormatter(
      final String format
  )
  {
    if (format.equalsIgnoreCase("millis")) {
      return new Function<Long, String>()
      {
        @Override
        public String apply(Long input)
        {
          return "" + input;
        }
      };
    } else if (format.equalsIgnoreCase("posix")) {
      return new Function<Long, String>()
      {
        @Override
        public String apply(Long input)
        {
          return "" + input / 1000;
        }
      };
    } else if (format.equalsIgnoreCase("nano")) {
      return new Function<Long, String>()
      {
        @Override
        public String apply(Long input)
        {
          return "" + input * 1000000L;
        }
      };
    } else if (format.equalsIgnoreCase("ruby")) {
      return new Function<Long, String>()
      {
        @Override
        public String apply(Long input)
        {
          return String.format("%d.%3d000", input / 1000, input % 1000);
        }
      };
    } else if (format.equalsIgnoreCase("iso")) {
      final DateTimeFormatter formatter = ISODateTimeFormat.dateTime();
      return new Function<Long, String>()
      {
        @Override
        public String apply(Long input)
        {
          return formatter.print((input));
        }
      };
    } else {
      try {
        final DateTimeFormatter formatter = DateTimeFormat.forPattern(format);
        return new Function<Long, String>()
        {
          @Override
          public String apply(Long input)
          {
            return formatter.print(input);
          }
        };
      }
      catch (Exception e) {
        throw new IAE(e, "Unable to format timestamps with format [%s]", format);
      }
    }
  }
}
