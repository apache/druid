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

package org.apache.druid.testing.utils;

import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class QueryResultVerifier
{
  /**
   * Tests the actual vs the expected results for equality and returns a {@link ResultVerificationObject} containing the
   * result of the check. If fieldsToTest is not null and non empty, only the supplied fields would be tested for
   * equality. Else, the whole row is compared
   */
  public static ResultVerificationObject compareResults(
      Iterable<Map<String, Object>> actual,
      Iterable<Map<String, Object>> expected,
      List<String> fieldsToTest
  )
  {
    Iterator<Map<String, Object>> actualIter = actual.iterator();
    Iterator<Map<String, Object>> expectedIter = expected.iterator();

    int rowNumber = 1;
    while (actualIter.hasNext() && expectedIter.hasNext()) {
      Map<String, Object> actualRes = actualIter.next();
      Map<String, Object> expRes = expectedIter.next();

      if (fieldsToTest != null && !fieldsToTest.isEmpty()) {
        for (String field : fieldsToTest) {
          if (!actualRes.get(field).equals(expRes.get(field))) {
            String mismatchMessage = StringUtils.format(
                "Mismatch in row no. [%d], column [%s]. Expected: [%s], Actual: [%s]",
                rowNumber,
                field,
                expRes,
                actualRes
            );
            return new ResultVerificationObject(mismatchMessage);
          }
        }
      } else {
        if (!actualRes.equals(expRes)) {
          String mismatchMessage = StringUtils.format(
              "Mismatch in row no. [%d]. Expected: [%s], Actual: [%s]",
              rowNumber,
              expRes,
              actualRes
          );
          return new ResultVerificationObject(mismatchMessage);
        }
      }
      ++rowNumber;
    }

    if (actualIter.hasNext() || expectedIter.hasNext()) {
      String mismatchMessage =
          StringUtils.format(
              "Results size mismatch. The actual result contain %s rows than the expected result.",
              actualIter.hasNext() ? "more" : "less"
          );
      return new ResultVerificationObject(mismatchMessage);
    }
    return new ResultVerificationObject(null);
  }

  public static class ResultVerificationObject
  {
    @Nullable
    private final String errorMessage;

    ResultVerificationObject(@Nullable final String errorMessage)
    {
      this.errorMessage = errorMessage;
    }

    public boolean isSuccess()
    {
      return getErrorMessage() == null;
    }

    @Nullable
    public String getErrorMessage()
    {
      return errorMessage;
    }
  }
}
