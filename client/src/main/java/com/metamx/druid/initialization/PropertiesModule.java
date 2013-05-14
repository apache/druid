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

package com.metamx.druid.initialization;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.metamx.common.config.Config;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.curator.CuratorConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.skife.config.ConfigurationObjectFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 */
public class PropertiesModule implements Module
{
  private static final Logger log = new Logger(PropertiesModule.class);

  private final String propertiesFile;

  public PropertiesModule(String propertiesFile)
  {
    this.propertiesFile = propertiesFile;
  }

  @Override
  public void configure(Binder binder)
  {
    final Properties zkProps = new Properties();
    final Properties fileProps = new Properties(zkProps);

    // Note that zookeeper coordinates must be either in cmdLine or in runtime.properties
    Properties sp = System.getProperties();

    Properties tmp_props = new Properties(fileProps); // the head of the 3 level Properties chain
    tmp_props.putAll(sp);

    final InputStream stream = ClassLoader.getSystemResourceAsStream(propertiesFile);
    if (stream == null) {
      log.info("%s not found on classpath, relying only on system properties and zookeeper.", propertiesFile);
    } else {
      log.info("Loading properties from %s", propertiesFile);
      try {
        try {
          fileProps.load(stream);
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
      finally {
        Closeables.closeQuietly(stream);
      }
    }

    // log properties from file; stringPropertyNames() would normally cascade down into the sub Properties objects, but
    //    zkProps (the parent level) is empty at this point so it will only log properties from runtime.properties
    for (String prop : fileProps.stringPropertyNames()) {
      log.info("Loaded(runtime.properties) Property[%s] as [%s]", prop, fileProps.getProperty(prop));
    }

    final String zkHostsProperty = "druid.zk.service.host";

    if (tmp_props.getProperty(zkHostsProperty) != null) {
      final ConfigurationObjectFactory factory = Config.createFactory(tmp_props);

      ZkPathsConfig config;
      try {
        config = factory.build(ZkPathsConfig.class);
      }
      catch (IllegalArgumentException e) {
        log.warn(e, "Unable to build ZkPathsConfig.  Cannot load properties from ZK.");
        config = null;
      }

      if (config != null) {
        Lifecycle lifecycle = new Lifecycle();
        try {
          CuratorFramework curator = Initialization.makeCuratorFramework(factory.build(CuratorConfig.class), lifecycle);

          lifecycle.start();

          final Stat stat = curator.checkExists().forPath(config.getPropertiesPath());
          if (stat != null) {
            final byte[] data = curator.getData().forPath(config.getPropertiesPath());
            zkProps.load(new InputStreamReader(new ByteArrayInputStream(data), Charsets.UTF_8));
          }

          // log properties from zk
          for (String prop : zkProps.stringPropertyNames()) {
            log.info("Loaded(zk) Property[%s] as [%s]", prop, zkProps.getProperty(prop));
          }
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
        finally {
          lifecycle.stop();
        }
      }
    } else {
      log.warn("property[%s] not set, skipping ZK-specified properties.", zkHostsProperty);
    }

    binder.bind(Properties.class).toInstance(tmp_props);
  }
}
