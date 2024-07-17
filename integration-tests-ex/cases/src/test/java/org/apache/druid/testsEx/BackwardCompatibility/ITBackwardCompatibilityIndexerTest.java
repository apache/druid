package org.apache.druid.testsEx.BackwardCompatibility;

import org.apache.druid.testsEx.categories.BackwardCompatibility;
import org.apache.druid.testsEx.categories.BatchIndex;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.apache.druid.testsEx.indexer.IndexerTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(DruidTestRunner.class)
@Category({BackwardCompatibility.class})
public class ITBackwardCompatibilityIndexerTest extends IndexerTest
{
}
