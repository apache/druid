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

package org.apache.druid.indexing.common.task;

import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.java.util.common.UOE;
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
    public boolean isReady(TaskActionClient taskActionClient)
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
    public TaskStatus run(TaskToolbox toolbox)
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
  public void testGetInputSourceResources()
  {
    Assert.assertThrows(
        UOE.class,
        TASK::getInputSourceResources
    );
  }
}
