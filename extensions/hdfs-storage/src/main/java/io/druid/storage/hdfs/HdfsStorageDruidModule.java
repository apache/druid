/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.storage.hdfs;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.name.Names;
import io.druid.guice.Binders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.initialization.DruidModule;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.storage.hdfs.tasklog.HdfsTaskLogs;
import io.druid.storage.hdfs.tasklog.HdfsTaskLogsConfig;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Properties;

/**
 */
public class HdfsStorageDruidModule implements DruidModule
{
  private Properties props = null;

  @Inject
  public void setProperties(Properties props)
  {
    this.props = props;
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  @Override
  public void configure(Binder binder)
  {
    binder.bind(Key.get(DataSegmentPuller.class, Names.named("hdfs")))
          .to(HdfsDataSegmentPuller.class)
          .in(LazySingleton.class);

    Binders.dataSegmentPusherBinder(binder).addBinding("hdfs").to(HdfsDataSegmentPusher.class).in(LazySingleton.class);
    Binders.dataSegmentKillerBinder(binder).addBinding("hdfs").to(HdfsDataSegmentKiller.class).in(LazySingleton.class);

    final Configuration conf = new Configuration();
    if (props != null) {
      for (String propName : System.getProperties().stringPropertyNames()) {
        if (propName.startsWith("hadoop.")) {
          conf.set(propName.substring("hadoop.".length()), System.getProperty(propName));
        }
      }
    }

    binder.bind(Configuration.class).toInstance(conf);
    JsonConfigProvider.bind(binder, "druid.storage", HdfsDataSegmentPusherConfig.class);

    Binders.taskLogsBinder(binder).addBinding("hdfs").to(HdfsTaskLogs.class);
    JsonConfigProvider.bind(binder, "druid.indexer.logs", HdfsTaskLogsConfig.class);
    binder.bind(HdfsTaskLogs.class).in(LazySingleton.class);
  }
}
