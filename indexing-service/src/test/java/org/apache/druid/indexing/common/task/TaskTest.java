package org.apache.druid.indexing.common.task;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TaskTest
{
  private static final Task TASK = new Task()
  {
    @Override
    public String getId()
    {
      return null;
    }

    @Override
    public String getGroupId()
    {
      return null;
    }

    @Override
    public TaskResource getTaskResource()
    {
      return null;
    }

    @Override
    public String getType()
    {
      return null;
    }

    @Override
    public String getNodeType()
    {
      return null;
    }

    @Override
    public String getDataSource()
    {
      return null;
    }

    @Override
    public <T> QueryRunner<T> getQueryRunner(Query<T> query)
    {
      return null;
    }

    @Override
    public boolean supportsQueries()
    {
      return false;
    }

    @Override
    public String getClasspathPrefix()
    {
      return null;
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient) throws Exception
    {
      return false;
    }

    @Override
    public boolean canRestore()
    {
      return false;
    }

    @Override
    public void stopGracefully(TaskConfig taskConfig)
    {

    }

    @Override
    public TaskStatus run(TaskToolbox toolbox) throws Exception
    {
      return null;
    }

    @Override
    public Map<String, Object> getContext()
    {
      return null;
    }
  };

  @Test
  public void testGetInputSourceTypes()
  {
    Assert.assertEquals(ImmutableSet.of(), TASK.getInputSourceTypes());
  }

  @Test
  public void testUsesFirehose()
  {
    Assert.assertFalse(TASK.usesFirehose());
  }
}
