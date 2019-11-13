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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.JvmUtils;
import org.apache.hadoop.yarn.util.ApplicationClassLoader;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;

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
        ImmutableList.of(),
        ImmutableMap.of()
    )
    {
      @Override
      public String getType()
      {
        return null;
      }

      @Override
      public boolean isReady(TaskActionClient taskActionClient)
      {
        return false;
      }

      @Override
      public void stopGracefully(TaskConfig taskConfig)
      {
      }

      @Override
      public boolean requireLockExistingSegments()
      {
        return true;
      }

      @Override
      public List<DataSegment> findSegmentsToLock(TaskActionClient taskActionClient, List<Interval> intervals)
      {
        return Collections.emptyList();
      }

      @Override
      public boolean isPerfectRollup()
      {
        return true;
      }

      @Nullable
      @Override
      public Granularity getSegmentGranularity()
      {
        return null;
      }

      @Override
      public TaskStatus runTask(TaskToolbox toolbox)
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
        null,
        null
    )).once();
    EasyMock.replay(toolbox);

    final ClassLoader classLoader = task.buildClassLoader(toolbox);
    assertClassLoaderIsSingular(classLoader);

    final Class<?> hadoopClazz = Class.forName("org.apache.hadoop.fs.FSDataInputStream", false, classLoader);
    assertClassLoaderIsSingular(hadoopClazz.getClassLoader());

    final Class<?> druidHadoopConfigClazz = Class.forName(
        "org.apache.druid.indexer.HadoopDruidIndexerConfig",
        false,
        classLoader
    );
    assertClassLoaderIsSingular(druidHadoopConfigClazz.getClassLoader());
  }

  public static void assertClassLoaderIsSingular(ClassLoader classLoader)
  {
    if (JvmUtils.isIsJava9Compatible()) {
      // See also https://docs.oracle.com/en/java/javase/11/migrate/index.html#JSMIG-GUID-A868D0B9-026F-4D46-B979-901834343F9E
      Assert.assertEquals("PlatformClassLoader", classLoader.getParent().getClass().getSimpleName());
    } else {
      // This is a check against the current HadoopTask which creates a single URLClassLoader with null parent
      Assert.assertNull(classLoader.getParent());
    }
    Assert.assertFalse(classLoader instanceof ApplicationClassLoader);
    Assert.assertTrue(classLoader instanceof URLClassLoader);

    final ClassLoader appLoader = HadoopDruidIndexerConfig.class.getClassLoader();
    Assert.assertNotEquals(
        StringUtils.format("ClassLoader [%s] is not isolated!", classLoader),
        appLoader,
        classLoader
    );
  }
}
