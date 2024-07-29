package org.apache.druid.testsEx.BackwardCompatibilityMain;

import org.apache.druid.testsEx.categories.BackwardCompatibilityMain;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.apache.druid.testsEx.msq.MultiStageQuery;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(DruidTestRunner.class)
@Category(BackwardCompatibilityMain.class)
public class ITBCMainMultiStageQuery extends MultiStageQuery
{
}
