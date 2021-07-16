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

package org.apache.druid.indexing.pulsar;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.indexing.pulsar.supervisor.PulsarSupervisorSpec;
import org.apache.druid.indexing.pulsar.supervisor.PulsarSupervisorTuningConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClientFactory;
import org.apache.druid.initialization.DruidModule;

import java.util.List;

public class PulsarIndexingServiceModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule(getClass().getSimpleName())
            .registerSubtypes(
                new NamedType(PulsarIndexTask.class, "index_pulsar"),
                new NamedType(PulsarDataSourceMetadata.class, "pulsar"),
                new NamedType(PulsarIndexTaskIOConfig.class, "pulsar"),
                new NamedType(PulsarIndexTaskTuningConfig.class, "pulsar"),
                new NamedType(PulsarSupervisorTuningConfig.class, "pulsar"),
                new NamedType(PulsarSupervisorSpec.class, "pulsar"),
                new NamedType(PulsarSamplerSpec.class, "pulsar")
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    binder.bind(
        new TypeLiteral<SeekableStreamIndexTaskClientFactory<PulsarIndexTaskClient>>()
        {
        }
    ).to(PulsarIndexTaskClientFactory.class).in(LazySingleton.class);
  }
}
