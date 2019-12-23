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

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.apache.druid.common.aws.AWSCredentialsConfig;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.indexing.kinesis.supervisor.KinesisSupervisorSpec;
import org.apache.druid.indexing.kinesis.supervisor.KinesisSupervisorTuningConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClientFactory;
import org.apache.druid.initialization.DruidModule;

import java.util.List;

public class KinesisIndexingServiceModule implements DruidModule
{
  public static final String AWS_SCOPE = "kinesis";
  static final String PROPERTY_BASE = "druid.kinesis";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule(getClass().getSimpleName())
            .registerSubtypes(
                new NamedType(KinesisIndexTask.class, "index_kinesis"),
                new NamedType(KinesisDataSourceMetadata.class, "kinesis"),
                new NamedType(KinesisIndexTaskIOConfig.class, "kinesis"),
                new NamedType(KinesisSupervisorTuningConfig.class, "kinesis"),
                new NamedType(KinesisSupervisorSpec.class, "kinesis"),
                new NamedType(KinesisSamplerSpec.class, "kinesis")
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    binder.bind(
        new TypeLiteral<SeekableStreamIndexTaskClientFactory<KinesisIndexTaskClient>>()
        {
        }
    ).to(KinesisIndexTaskClientFactory.class).in(LazySingleton.class);

    JsonConfigProvider.bind(binder, PROPERTY_BASE, AWSCredentialsConfig.class, Names.named(AWS_SCOPE));
  }
}
