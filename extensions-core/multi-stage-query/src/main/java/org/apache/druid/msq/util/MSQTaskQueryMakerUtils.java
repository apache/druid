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
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.exec.SegmentSource;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;

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
   * Validates that each element of {@link MultiStageQueryContext#CTX_SORT_ORDER}, if provided, appears in the
   * final output.
   */
  public static void validateContextSortOrderColumnsExist(
      final List<String> contextSortOrder,
      final Set<String> allOutputColumns
  )
  {
    for (final String column : contextSortOrder) {
      if (!allOutputColumns.contains(column)) {
        throw InvalidSqlInput.exception(
            "Column[%s] from context parameter[%s] does not appear in the query output",
            column,
            MultiStageQueryContext.CTX_SORT_ORDER
        );
      }
    }
  }

  /**
   * Validates that a query does not read from a datasource that it is ingesting data into, if realtime segments are
   * being queried.
   */
  public static void validateRealtimeReindex(final MSQSpec querySpec)
  {
    final SegmentSource segmentSources = MultiStageQueryContext.getSegmentSources(querySpec.getQuery().context());
    if (MSQControllerTask.isReplaceInputDataSourceTask(querySpec) && SegmentSource.REALTIME.equals(segmentSources)) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build("Cannot ingest into datasource[%s] since it is also being queried from, with "
                                 + "REALTIME segments included. Ingest to a different datasource, or disable querying "
                                 + "of realtime segments by modifying [%s] in the query context.",
                                 ((DataSourceMSQDestination) querySpec.getDestination()).getDataSource(),
                                 MultiStageQueryContext.CTX_INCLUDE_SEGMENT_SOURCE
                          );
    }
  }
}
