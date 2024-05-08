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

package org.apache.druid.delta.common;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import org.apache.druid.delta.input.DeltaInputSource;
import org.apache.druid.error.DruidException;
import org.apache.druid.initialization.DruidModule;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.util.Collections;
import java.util.List;

public class DeltaLakeDruidModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("DeltaLakeDruidModule")
            .registerSubtypes(
                new NamedType(DeltaInputSource.class, DeltaInputSource.TYPE_KEY)
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    final Configuration conf = new Configuration();
    conf.setClassLoader(getClass().getClassLoader());

    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      FileSystem.get(conf);
    }
    catch (Exception ex) {
      throw DruidException.forPersona(DruidException.Persona.DEVELOPER)
                          .ofCategory(DruidException.Category.UNCATEGORIZED)
                          .build(ex, "Problem during fileSystem class level initialization");
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

}

