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

package io.druid.query.topn;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.StringUtils;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

public class AlphaNumericTopNMetricSpec extends LexicographicTopNMetricSpec
{
  private static final byte CACHE_TYPE_ID = 0x2;

  protected static Comparator<String> comparator = new Comparator<String>()
  {
    // This code is based on https://github.com/amjjd/java-alphanum, see NOTICE file for more information
    public int compare(String str1, String str2)
    {
      int[] pos = {0, 0};

      if (str1 == null) {
        return -1;
      } else if (str2 == null) {
        return 1;
      } else if (str1.length() == 0) {
        return str2.length() == 0 ? 0 : -1;
      } else if (str2.length() == 0) {
        return 1;
      }

      while (pos[0] < str1.length() && pos[1] < str2.length()) {
        int ch1 = str1.codePointAt(pos[0]);
        int ch2 = str2.codePointAt(pos[1]);

        int result = 0;

        if (isDigit(ch1)) {
          result = isDigit(ch2) ? compareNumbers(str1, str2, pos) : -1;
        } else {
          result = isDigit(ch2) ? 1 : compareNonNumeric(str1, str2, pos);
        }

        if (result != 0) {
          return result;
        }
      }

      return str1.length() - str2.length();
    }

    private int compareNumbers(String str0, String str1, int[] pos)
    {
      int delta = 0;
      int zeroes0 = 0, zeroes1 = 0;
      int ch0 = -1, ch1 = -1;

      // Skip leading zeroes, but keep a count of them.
      while (pos[0] < str0.length() && isZero(ch0 = str0.codePointAt(pos[0]))) {
        zeroes0++;
        pos[0] += Character.charCount(ch0);
      }
      while (pos[1] < str1.length() && isZero(ch1 = str1.codePointAt(pos[1]))) {
        zeroes1++;
        pos[1] += Character.charCount(ch1);
      }

      // If one sequence contains more significant digits than the
      // other, it's a larger number. In case they turn out to have
      // equal lengths, we compare digits at each position; the first
      // unequal pair determines which is the bigger number.
      while (true) {
        boolean noMoreDigits0 = (ch0 < 0) || !isDigit(ch0);
        boolean noMoreDigits1 = (ch1 < 0) || !isDigit(ch1);

        if (noMoreDigits0 && noMoreDigits1) {
          return delta != 0 ? delta : zeroes0 - zeroes1;
        } else if (noMoreDigits0) {
          return -1;
        } else if (noMoreDigits1) {
          return 1;
        } else if (delta == 0 && ch0 != ch1) {
          delta = valueOf(ch0) - valueOf(ch1);
        }

        if (pos[0] < str0.length()) {
          ch0 = str0.codePointAt(pos[0]);
          if (isDigit(ch0)) {
            pos[0] += Character.charCount(ch0);
          } else {
            ch0 = -1;
          }
        } else {
          ch0 = -1;
        }

        if (pos[1] < str1.length()) {
          ch1 = str1.codePointAt(pos[1]);
          if (isDigit(ch1)) {
            pos[1] += Character.charCount(ch1);
          } else {
            ch1 = -1;
          }
        } else {
          ch1 = -1;
        }
      }
    }

    private boolean isDigit(int ch)
    {
      return (ch >= '0' && ch <= '9') ||
             (ch >= '\u0660' && ch <= '\u0669') ||
             (ch >= '\u06F0' && ch <= '\u06F9') ||
             (ch >= '\u0966' && ch <= '\u096F') ||
             (ch >= '\uFF10' && ch <= '\uFF19');
    }

    private boolean isZero(int ch)
    {
      return ch == '0' || ch == '\u0660' || ch == '\u06F0' || ch == '\u0966' || ch == '\uFF10';
    }

    private int valueOf(int digit)
    {
      if (digit <= '9') {
        return digit - '0';
      }
      if (digit <= '\u0669') {
        return digit - '\u0660';
      }
      if (digit <= '\u06F9') {
        return digit - '\u06F0';
      }
      if (digit <= '\u096F') {
        return digit - '\u0966';
      }
      if (digit <= '\uFF19') {
        return digit - '\uFF10';
      }

      return digit;
    }

    private int compareNonNumeric(String str0, String str1, int[] pos)
    {
      // find the end of both non-numeric substrings
      int start0 = pos[0];
      int ch0 = str0.codePointAt(pos[0]);
      pos[0] += Character.charCount(ch0);
      while (pos[0] < str0.length() && !isDigit(ch0 = str0.codePointAt(pos[0]))) {
        pos[0] += Character.charCount(ch0);
      }

      int start1 = pos[1];
      int ch1 = str1.codePointAt(pos[1]);
      pos[1] += Character.charCount(ch1);
      while (pos[1] < str1.length() && !isDigit(ch1 = str1.codePointAt(pos[1]))) {
        pos[1] += Character.charCount(ch1);
      }

      // compare the substrings
      return String.CASE_INSENSITIVE_ORDER.compare(str0.substring(start0, pos[0]), str1.substring(start1, pos[1]));
    }
  };

  @JsonCreator
  public AlphaNumericTopNMetricSpec(
      @JsonProperty("previousStop") String previousStop
  )
  {
    super(previousStop);
  }

  @Override
  public Comparator getComparator(List<AggregatorFactory> aggregatorSpecs, List<PostAggregator> postAggregatorSpecs)
  {
    return comparator;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] previousStopBytes = getPreviousStop() == null ? new byte[]{} : StringUtils.toUtf8(getPreviousStop());

    return ByteBuffer.allocate(1 + previousStopBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(previousStopBytes)
                     .array();
  }

  @Override
  public <T> TopNMetricSpecBuilder<T> configureOptimizer(TopNMetricSpecBuilder<T> builder)
  {
    return builder;
  }

  @Override
  public String toString()
  {
    return "AlphaNumericTopNMetricSpec{" +
           "previousStop='" + getPreviousStop() + '\'' +
           '}';
  }
}
