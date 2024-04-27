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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.druid.common.aws.AWSCredentialsConfig;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;

import java.util.Arrays;
import java.util.Collections;

public class KinesisIndexTaskSerdeTest
{
  private static final DataSchema DATA_SCHEMA = new DataSchema("dataSource", null, null, null, null, null, null, null);
  private static final KinesisIndexTaskTuningConfig TUNING_CONFIG = new KinesisIndexTaskTuningConfig(
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null
  );
  private static final KinesisIndexTaskIOConfig IO_CONFIG = new KinesisIndexTaskIOConfig(
      0,
      "baseSequenceName",
      new SeekableStreamStartSequenceNumbers<>("stream", Collections.emptyMap(), null),
      new SeekableStreamEndSequenceNumbers<>("stream", Collections.emptyMap()),
      null,
      null,
      null,
      null,
      "endpoint",
      null,
      null,
      null
  );
  private static final String ACCESS_KEY = "test-access-key";
  private static final String SECRET_KEY = "test-secret-key";
  private static final String FILE_SESSION_CREDENTIALS = "test-file-session-credentials";

  @Rule
  public ProvideSystemProperty properties = new ProvideSystemProperty(
      KinesisIndexingServiceModule.PROPERTY_BASE + ".accessKey",
      ACCESS_KEY
  ).and(
      KinesisIndexingServiceModule.PROPERTY_BASE + ".secretKey",
      SECRET_KEY
  ).and(
      KinesisIndexingServiceModule.PROPERTY_BASE + ".fileSessionCredentials",
      FILE_SESSION_CREDENTIALS
  );

  @Test
  public void injectsProperAwsCredentialsConfig() throws Exception
  {
    KinesisIndexTask target = new KinesisIndexTask(
        "id",
        null,
        DATA_SCHEMA,
        TUNING_CONFIG,
        IO_CONFIG,
        null,
        false,
        null
    );
    ObjectMapper objectMapper = createObjectMapper();
    String serialized = objectMapper.writeValueAsString(target);
    KinesisIndexTask deserialized = objectMapper.readValue(serialized, KinesisIndexTask.class);

    AWSCredentialsConfig awsCredentialsConfig = deserialized.getAwsCredentialsConfig();
    Assert.assertEquals(ACCESS_KEY, awsCredentialsConfig.getAccessKey().getPassword());
    Assert.assertEquals(SECRET_KEY, awsCredentialsConfig.getSecretKey().getPassword());
    Assert.assertEquals(FILE_SESSION_CREDENTIALS, awsCredentialsConfig.getFileSessionCredentials());
    Assert.assertEquals(
        Collections.singleton(
            new ResourceAction(new Resource(
                KinesisIndexingServiceModule.SCHEME,
                ResourceType.EXTERNAL
            ), Action.READ)),
        target.getInputSourceResources()
    );
  }

  private static ObjectMapper createObjectMapper()
  {
    DruidModule module = new KinesisIndexingServiceModule();
    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        Arrays.asList(
            module,
            (Module) binder -> {
              binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8000);
              binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(9000);
              binder.bind(ChatHandlerProvider.class).toInstance(new NoopChatHandlerProvider());
              binder.bind(RowIngestionMetersFactory.class).toInstance(new DropwizardRowIngestionMetersFactory());
              binder.bind(AppenderatorsManager.class).toInstance(new TestAppenderatorsManager());
            }
        )
    );
    ObjectMapper objectMapper = injector.getInstance(ObjectMapper.class);
    module.getJacksonModules().forEach(objectMapper::registerModule);
    return objectMapper;
  }
}
