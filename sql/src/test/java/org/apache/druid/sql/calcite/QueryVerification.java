package org.apache.druid.sql.calcite;

public class QueryVerification
{
  public static QueryResultsVerifierFactory ofResults(QueryResultsVerifier verifier) {
    return new QueryResultsVerifierFactory(verifier);
  }

  public interface QueryResultsVerifier {
    void verifyResults(QueryTestRunner.QueryResults results);
  }

  public static class QueryResultsVerifierFactory implements QueryTestRunner.QueryVerifyStepFactory
  {
    private final QueryResultsVerifier verifier;

    public QueryResultsVerifierFactory(
        QueryResultsVerifier verifier
    ) {
      this.verifier = verifier;
    }

    @Override
    public QueryTestRunner.QueryVerifyStep make(QueryTestRunner.ExecuteQuery execStep)
    {
      return () -> {
        for (QueryTestRunner.QueryResults queryResults : execStep.results()) {
          verifier.verifyResults(queryResults);
        }
      };
    }
  }
}
