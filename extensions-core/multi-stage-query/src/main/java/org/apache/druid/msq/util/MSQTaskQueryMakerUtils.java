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

package org.apache.druid.msq.util;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnHolder;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MSQTaskQueryMakerUtils
{

  public static final Set<String> SENSISTIVE_JSON_KEYS = ImmutableSet.of("accessKeyId", "secretAccessKey");
  public static final Set<Pattern> SENSITIVE_KEYS_REGEX_PATTERNS = SENSISTIVE_JSON_KEYS.stream()
                                                                                       .map(sensitiveKey ->
                                                                                                 Pattern.compile(
                                                                                                     StringUtils.format(
                                                                                                         "\\\\\"%s\\\\\"(\\s)*:(\\s)*(?<sensitive>\\{(\\s)*(\\S)+?(\\s)*\\})",
                                                                                                         sensitiveKey
                                                                                                     ),
                                                                                                     Pattern.CASE_INSENSITIVE
                                                                                                 ))
                                                                                       .collect(Collectors.toSet());

  /**
   * This method masks the sensitive json keys that might be present in the SQL query matching the regex
   * {@code key(\s)+:(\s)+{sensitive_data}}
   * The regex pattern matches a json entry of form "key":{value} and replaces it with "key":\<masked\>
   * It checks the sensitive keys for the match, greedily matches the first occuring brace pair ("{" and "}")
   * into a regex group named "sensitive" and performs a string replace on the group. The whitespaces are accounted
   * for in the regex.
   */
  public static String maskSensitiveJsonKeys(String sqlQuery)
  {
    StringBuilder maskedSqlQuery = new StringBuilder(sqlQuery);
    for (Pattern p : SENSITIVE_KEYS_REGEX_PATTERNS) {
      Matcher m = p.matcher(sqlQuery);
      while (m.find()) {
        String sensitiveData = m.group("sensitive");
        int start = maskedSqlQuery.indexOf(sensitiveData);
        int end = start + sensitiveData.length();
        maskedSqlQuery.replace(start, end, "<masked>");
      }
    }
    return maskedSqlQuery.toString();
  }

  /**
   * Validates if each element of the sort order appears in the final output and if it is not empty then it starts with the
   * __time column
   */
  public static void validateSegmentSortOrder(final List<String> sortOrder, final Collection<String> allOutputColumns)
  {
    final Set<String> allOutputColumnsSet = new HashSet<>(allOutputColumns);

    for (final String column : sortOrder) {
      if (!allOutputColumnsSet.contains(column)) {
        throw new IAE("Column [%s] in segment sort order does not appear in the query output", column);
      }
    }

    if (sortOrder.size() > 0
        && allOutputColumns.contains(ColumnHolder.TIME_COLUMN_NAME)
        && !ColumnHolder.TIME_COLUMN_NAME.equals(sortOrder.get(0))) {
      throw new IAE("Segment sort order must begin with column [%s]", ColumnHolder.TIME_COLUMN_NAME);
    }
  }
}
