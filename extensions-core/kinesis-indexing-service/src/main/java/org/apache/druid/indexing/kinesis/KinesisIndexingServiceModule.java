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
import com.google.inject.name.Names;
import org.apache.druid.common.aws.AWSCredentialsConfig;
import org.apache.druid.data.input.kinesis.KinesisInputFormat;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.indexing.kinesis.supervisor.KinesisSupervisorSpec;
import org.apache.druid.indexing.kinesis.supervisor.KinesisSupervisorTuningConfig;
import org.apache.druid.initialization.DruidModule;

import java.util.List;

public class KinesisIndexingServiceModule implements DruidModule
{
  public static final String AWS_SCOPE = "kinesis";
  public static final String SCHEME = "kinesis";
  static final String PROPERTY_BASE = "druid.kinesis";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule(getClass().getSimpleName())
            .registerSubtypes(
                new NamedType(KinesisIndexTask.class, "index_kinesis"),
                new NamedType(KinesisDataSourceMetadata.class, SCHEME),
                new NamedType(KinesisIndexTaskIOConfig.class, SCHEME),
                new NamedType(KinesisSupervisorTuningConfig.class, SCHEME),
                new NamedType(KinesisSupervisorSpec.class, SCHEME),
                new NamedType(KinesisSamplerSpec.class, SCHEME),
                new NamedType(KinesisInputFormat.class, SCHEME)
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, PROPERTY_BASE, AWSCredentialsConfig.class, Names.named(AWS_SCOPE));
  }
}
