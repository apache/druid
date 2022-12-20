package org.apache.druid.testsEx.msq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import java.util.List;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.testsEx.categories.GcsDeepStorage;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.apache.druid.testsEx.indexer.AbstractGcsInputSourceParallelIndexTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;

@RunWith(DruidTestRunner.class)
@Category(GcsDeepStorage.class)
public class ITGcsSQLBasedIngestionTest extends AbstractGcsInputSourceParallelIndexTest
{
  private static final String CLOUD_INGEST_SQL = "/multi-stage-query/wikipedia_cloud_index_msq.sql";
  private static final String INDEX_QUERIES_FILE = "/multi-stage-query/wikipedia_index_queries.json";

  @Inject
  @Json
  protected ObjectMapper jsonMapper;

  @Test
  @Parameters(method = "resources")
  @TestCaseName("Test_{index} ({0})")
  public void testSQLBasedBatchIngestion(Pair<String, List> GcsInputSource)
  {
    doMSQTest(GcsInputSource, CLOUD_INGEST_SQL, INDEX_QUERIES_FILE, "gcs");
  }
}
