/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.storage.hdfs;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import io.druid.guice.Binders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.initialization.DruidModule;
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
    Binders.dataSegmentPullerBinder(binder).addBinding("hdfs").to(HdfsDataSegmentPuller.class).in(LazySingleton.class);
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
