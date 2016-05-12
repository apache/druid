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

package io.druid.guice;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.metamx.common.IAE;
import com.metamx.common.logger.Logger;
import io.druid.initialization.DruidModule;
import io.druid.segment.data.GenericIndexedWriterFactory;
import io.druid.segment.data.GenericIndexedWriterV1Factory;
import io.druid.segment.data.GenericIndexedWriterV2Factory;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class GenericIndexedWriterFactoryModule implements DruidModule
{
  private static final Logger log = new Logger(GenericIndexedWriterFactoryModule.class);
  private final static String GENERIC_INDEXING_VERSION_PROPERTY = "druid.indexer.genericIndexed.version";
  private final Properties properties;

  public GenericIndexedWriterFactoryModule(Properties properties)
  {
    this.properties = properties;
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  @Override
  public void configure(Binder binder)
  {
    final String genericIndexedVersion = properties.getProperty(GENERIC_INDEXING_VERSION_PROPERTY, "V1");
    switch (genericIndexedVersion.toUpperCase()) {
      case "V1":
        binder.bind(GenericIndexedWriterFactory.class).toInstance(new GenericIndexedWriterV1Factory());
        break;
      case "V2":
        binder.bind(GenericIndexedWriterFactory.class).toInstance(new GenericIndexedWriterV2Factory());
        break;
      default:
        throw new IAE("Unknown version [%s] for GenericIndexed", genericIndexedVersion);
    }
    log.info("Will use version: [%s] for writing GenericIndexed", genericIndexedVersion);
  }
}
