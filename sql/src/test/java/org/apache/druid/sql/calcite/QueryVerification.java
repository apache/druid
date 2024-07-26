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

package org.apache.druid.sql.calcite;

public class QueryVerification
{
  public static QueryResultsVerifierFactory ofResults(QueryResultsVerifier verifier)
  {
    return new QueryResultsVerifierFactory(verifier);
  }

  public interface QueryResultsVerifier
  {
    void verifyResults(QueryTestRunner.QueryResults results) throws Exception;
  }

  public static class QueryResultsVerifierFactory implements QueryTestRunner.QueryVerifyStepFactory
  {
    private final QueryResultsVerifier verifier;

    public QueryResultsVerifierFactory(
        QueryResultsVerifier verifier
    )
    {
      this.verifier = verifier;
    }

    @Override
    public QueryTestRunner.QueryVerifyStep make(QueryTestRunner.BaseExecuteQuery execStep)
    {
      return () -> {
        for (QueryTestRunner.QueryResults queryResults : execStep.results()) {
          try {
            verifier.verifyResults(queryResults);
          }
          catch (Exception e) {
            throw new RuntimeException("Exception during verification!", e);
          }
        }
      };
    }
  }
}
