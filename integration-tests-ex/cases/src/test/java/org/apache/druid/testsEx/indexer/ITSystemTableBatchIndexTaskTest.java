package org.apache.druid.testsEx.indexer;

import org.apache.druid.testsEx.categories.S3DeepStorage;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(DruidTestRunner.class)
@Category(S3DeepStorage.class)
public class ITSystemTableBatchIndexTaskTest extends SystemTableBatchIndexTaskTest
{
}
