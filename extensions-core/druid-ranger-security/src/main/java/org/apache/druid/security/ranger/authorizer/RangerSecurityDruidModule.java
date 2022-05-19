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

package org.apache.druid.security.ranger.authorizer;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.security.ranger.authorizer.guice.Ranger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class RangerSecurityDruidModule implements DruidModule
{
  private Properties props = null;

  @Override
  public void configure(Binder binder)
  {
    // this block of code is common among extensions that use Hadoop things but are not running in Hadoop, in order
    // to properly initialize everything

    final Configuration conf = new Configuration();

    // Set explicit CL. Otherwise it'll try to use thread context CL, which may not have all of our dependencies.
    conf.setClassLoader(getClass().getClassLoader());

    // Ensure that FileSystem class level initialization happens with correct CL
    // See https://github.com/apache/druid/issues/1714
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      FileSystem.get(conf);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }

    if (props != null) {
      for (String propName : props.stringPropertyNames()) {
        if (propName.startsWith("hadoop.")) {
          conf.set(propName.substring("hadoop.".length()), props.getProperty(propName));
        }
      }
    }

    binder.bind(Configuration.class).annotatedWith(Ranger.class).toInstance(conf);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
      new SimpleModule("RangerDruidSecurity").registerSubtypes(RangerAuthorizer.class)
    );
  }

  @Inject
  public void setProperties(Properties props)
  {
    this.props = props;
  }

}
