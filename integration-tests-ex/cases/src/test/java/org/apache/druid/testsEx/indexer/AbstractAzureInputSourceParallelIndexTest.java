package org.apache.druid.testsEx.indexer;

import static org.junit.Assert.fail;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.utils.AzureTestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class AbstractAzureInputSourceParallelIndexTest extends AbstractCloudInputSourceParallelIndexTest
{
  private static final Logger LOG = new Logger(AbstractAzureInputSourceParallelIndexTest.class);

  static AzureTestUtil azure;

  @BeforeClass
  public static void uploadDataFilesToAzure()
  {
    try {
      azure = new AzureTestUtil();
      // Creating a container with name set in AZURE_CONTAINER env variable.
      azure.createStorageContainer();
      String localPath = "resources/data/batch_index/json/";
      for (String file : fileList()) {
        azure.uploadFileToContainer(localPath + file);
      }
    }
    catch (Exception e) {
      LOG.error(e, "Unable to upload files to azure");
      // Fail if exception
      fail();
    }
  }

  @AfterClass
  public static void deleteDataFilesFromAzure()
  {
    try {
      // Deleting uploaded data files
      azure.deleteStorageContainer();
    }
    catch (Exception e) {
    LOG.warn(e, "Unable to delete container in azure");
  }
  }
}
