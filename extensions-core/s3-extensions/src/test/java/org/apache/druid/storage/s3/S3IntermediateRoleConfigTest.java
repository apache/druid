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

package org.apache.druid.storage.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.UUID;

public class S3IntermediateRoleConfigTest
{
  private static final String PROPERTY_PREFIX = UUID.randomUUID().toString();
  private static final String INTERMEDIATE_ROLE_ARN = "arn:aws:iam::123456789012:role/intermediate";

  private final Properties properties = new Properties();

  @BeforeEach
  public void setUp()
  {
    properties.clear();
  }

  @AfterEach
  public void tearDown()
  {
    properties.clear();
  }

  private Injector createInjector()
  {
    return Guice.createInjector(
        binder -> {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test/s3");
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          // The mapper matters: see testSiblingPropertiesUnderTheSamePrefixAreIgnored.
          binder.bind(ObjectMapper.class).toInstance(new DefaultObjectMapper());
          binder.bind(JsonConfigurator.class).in(LazySingleton.class);
          binder.bind(Properties.class).toInstance(properties);
          JsonConfigProvider.bind(binder, PROPERTY_PREFIX, S3IntermediateRoleConfig.class);
        }
    );
  }

  @Test
  public void testIntermediateAssumeRoleArnIsUnsetByDefault()
  {
    final S3IntermediateRoleConfig config = createInjector().getInstance(S3IntermediateRoleConfig.class);
    Assertions.assertNull(config.getIntermediateAssumeRoleArn());
  }

  @Test
  public void testIntermediateAssumeRoleArnIsBoundFromProperties()
  {
    properties.put(PROPERTY_PREFIX + ".intermediateAssumeRoleArn", INTERMEDIATE_ROLE_ARN);

    final S3IntermediateRoleConfig config = createInjector().getInstance(S3IntermediateRoleConfig.class);
    Assertions.assertEquals(INTERMEDIATE_ROLE_ARN, config.getIntermediateAssumeRoleArn());
  }

  @Test
  public void testSiblingPropertiesUnderTheSamePrefixAreIgnored()
  {
    properties.put(PROPERTY_PREFIX + ".intermediateAssumeRoleArn", INTERMEDIATE_ROLE_ARN);
    properties.put(PROPERTY_PREFIX + ".accessKey", "someAccessKey");
    properties.put(PROPERTY_PREFIX + ".retryMode", "adaptive");

    final S3IntermediateRoleConfig config = createInjector().getInstance(S3IntermediateRoleConfig.class);
    Assertions.assertEquals(INTERMEDIATE_ROLE_ARN, config.getIntermediateAssumeRoleArn());
  }
}
