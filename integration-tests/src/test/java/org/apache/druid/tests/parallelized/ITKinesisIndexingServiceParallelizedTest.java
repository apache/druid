package org.apache.druid.tests.parallelized;

import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.tests.indexer.AbstractKinesisIndexingServiceTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

@Test(groups = TestNGGroup.KINESIS_INDEX)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITKinesisIndexingServiceParallelizedTest extends AbstractKinesisIndexingServiceTest
{
  @Override
  public String getTestNamePrefix()
  {
    return "kinesis_parallelized";
  }

  @BeforeClass
  public void beforeClass() throws Exception
  {
    doBeforeClass();
  }

  /**
   * This test can be run concurrently with other tests as it creates/modifies/teardowns a unique datasource
   * and supervisor maintained and scoped within this test only
   */
  @Test
  public void testKinesisTerminatedSupervisorAutoCleanup() throws Exception
  {
    doTestTerminatedSupervisorAutoCleanup(false);
  }
}
