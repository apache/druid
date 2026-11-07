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

package org.apache.druid.query.context;

import org.apache.commons.text.similarity.LevenshteinDistance;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.BadQueryContextException;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** Validates query context parameter names and values against the descriptor catalog. */
public final class QueryContextParameterValidator
{
  private QueryContextParameterValidator()
  {
  }

  /**
   * Validates the parameters of a query context in the alphabetical order of their names, and rejects the first
   * invalid one: a declared parameter whose value is invalid or, if {@code rejectUnknownParameters}, a name that is not
   * a declared parameter. The order is alphabetical so that a context is reported the same way however it reached
   * Druid, since some paths, such as {@link org.apache.druid.query.QueryContext}, sort the context. The error for an
   * unknown name suggests up to three declared parameters with similar names, as found by {@link #getHints}.
   *
   * @param rejectUnknownParameters whether to reject names that are not declared parameters, rather than ignore them
   *
   * @throws BadQueryContextException for the first invalid parameter
   */
  public static void validate(final Map<String, Object> parameters, final boolean rejectUnknownParameters)
  {
    final Map<String, QueryContextParameter<?>> declaredParameters = QueryContextParameters.ALL.get();
    parameters.keySet().stream().sorted().forEach(name -> {
      final QueryContextParameter<?> parameter = declaredParameters.get(name);
      if (parameter != null) {
        parameter.parse(parameters.get(name));
      } else if (rejectUnknownParameters) {
        throw unknownParameterException(name, declaredParameters.values());
      }
    });
  }

  private static BadQueryContextException unknownParameterException(
      final String name,
      final Collection<QueryContextParameter<?>> declaredParameters
  )
  {
    final List<String> hints = getHints(name, declaredParameters);
    if (hints.isEmpty()) {
      return new BadQueryContextException(StringUtils.format("Unknown query context parameter [%s].", name));
    }
    return new BadQueryContextException(
        StringUtils.format("Unknown query context parameter [%s]. Did you mean [%s]?", name, String.join(", ", hints))
    );
  }

  /**
   * Returns the names of up to three user-settable, non-deprecated parameters that are similar to {@code name}, closest
   * first. Follows the typo correction of ClickHouse's {@code NamePrompter}, which comes from Clang's
   * {@code SimpleTypoCorrector}: a name is similar when its case-insensitive Levenshtein distance from {@code name} is
   * at most one edit per three characters of {@code name}.
   */
  static List<String> getHints(final String name, final Collection<QueryContextParameter<?>> candidates)
  {
    final int mistakeFactor = (name.length() + 2) / 3;
    // The distance is -1 when it exceeds the mistake factor.
    final LevenshteinDistance levenshteinDistance = new LevenshteinDistance(mistakeFactor);
    final String lowerCaseName = StringUtils.toLowerCase(name);

    return candidates.stream()
                     .filter(candidate -> candidate.getVisibility() != QueryContextParameter.Visibility.SYSTEM
                                          && !candidate.isDeprecated())
                     // The length difference is a lower bound of the distance: skip names that cannot be close enough.
                     .filter(candidate -> {
                       final int lengthDifference = Math.abs(name.length() - candidate.getName().length());
                       return lengthDifference <= mistakeFactor
                              && (lengthDifference == 0 || name.length() / lengthDifference >= 3);
                     })
                     .map(candidate -> new Hint(
                         candidate.getName(),
                         levenshteinDistance.apply(lowerCaseName, StringUtils.toLowerCase(candidate.getName()))
                     ))
                     .filter(hint -> hint.distance() >= 0)
                     .sorted(Comparator.comparingInt(Hint::distance).thenComparing(Hint::name))
                     // Suggest at most three parameters.
                     .limit(3)
                     .map(Hint::name)
                     .toList();
  }

  private record Hint(String name, int distance)
  {
  }
}
