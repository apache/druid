package org.apache.druid.testsEx.config;

import org.apache.druid.java.util.common.logger.Logger;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class BaseJUnitRule
{
  private static final Logger LOG = new Logger(BaseJUnitRule.class);
  @Rule
  public TestWatcher watchman = new TestWatcher()
  {
    @Override
    public void starting(Description d)
    {
      LOG.info("RUNNING %s", d.getDisplayName());
    }

    @Override
    public void failed(Throwable e, Description d)
    {
      LOG.error("FAILED %s", d.getDisplayName());
    }

    @Override
    public void finished(Description d)
    {
      LOG.info("FINISHED %s", d.getDisplayName());
    }
  };
}
