/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.task;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.indexer.updater.HadoopDruidConverterConfig;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.java.util.common.StringUtils;
import org.apache.hadoop.yarn.util.ApplicationClassLoader;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URLClassLoader;

public class HadoopTaskTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testBuildClassLoader() throws Exception
  {
    final HadoopTask task = new HadoopTask(
        "taskId",
        "dataSource",
        ImmutableList.<String>of(),
        ImmutableMap.<String, Object>of()
    )
    {
      @Override
      public String getType()
      {
        return null;
      }

      @Override
      public boolean isReady(TaskActionClient taskActionClient) throws Exception
      {
        return false;
      }

      @Override
      public TaskStatus run(TaskToolbox toolbox) throws Exception
      {
        return null;
      }
    };
    final TaskToolbox toolbox = EasyMock.createStrictMock(TaskToolbox.class);
    EasyMock.expect(toolbox.getConfig()).andReturn(new TaskConfig(
        temporaryFolder.newFolder().toString(),
        null,
        null,
        null,
        ImmutableList.of("something:hadoop:1"),
        false,
        null,
        null
    )).once();
    EasyMock.replay(toolbox);

    final ClassLoader classLoader = task.buildClassLoader(toolbox);
    assertClassLoaderIsSingular(classLoader);

    final Class<?> hadoopClazz = Class.forName("org.apache.hadoop.fs.FSDataInputStream", false, classLoader);
    assertClassLoaderIsSingular(hadoopClazz.getClassLoader());

    final Class<?> druidHadoopConfigClazz = Class.forName("io.druid.indexer.HadoopDruidIndexerConfig", false, classLoader);
    assertClassLoaderIsSingular(druidHadoopConfigClazz.getClassLoader());
  }
  public static void assertClassLoaderIsSingular(ClassLoader classLoader)
  {
    // This is a check against the current HadoopTask which creates a single URLClassLoader with null parent
    Assert.assertNull(classLoader.getParent());
    Assert.assertFalse(classLoader instanceof ApplicationClassLoader);
    Assert.assertTrue(classLoader instanceof URLClassLoader);

    final ClassLoader appLoader = HadoopDruidConverterConfig.class.getClassLoader();
    Assert.assertNotEquals(StringUtils.format("ClassLoader [%s] is not isolated!", classLoader), appLoader, classLoader);
  }
}
