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

package org.apache.druid.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.ProvisionException;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Collections;

@RunWith(MockitoJUnitRunner.class)
public class StorageNodeModuleTest
{
  private static final boolean INJECT_SERVER_TYPE_CONFIG = true;

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Mock(answer = Answers.RETURNS_MOCKS)
  private ObjectMapper mapper;
  @Mock(answer = Answers.RETURNS_MOCKS)
  private DruidNode self;
  @Mock(answer = Answers.RETURNS_MOCKS)
  private ServerTypeConfig serverTypeConfig;
  @Mock
  private DruidProcessingConfig druidProcessingConfig;
  @Mock
  private SegmentLoaderConfig segmentLoaderConfig;
  @Mock
  private StorageLocationConfig storageLocation;

  private Injector injector;
  private StorageNodeModule target;

  @Before
  public void setUp()
  {
    Mockito.when(segmentLoaderConfig.getLocations()).thenReturn(Collections.singletonList(storageLocation));

    target = new StorageNodeModule();
    injector = makeInjector(INJECT_SERVER_TYPE_CONFIG);
  }

  @Test
  public void testIsSegmentCacheConfiguredIsInjected()
  {
    Boolean isSegmentCacheConfigured = injector.getInstance(
        Key.get(Boolean.class, Names.named(StorageNodeModule.IS_SEGMENT_CACHE_CONFIGURED))
    );
    Assert.assertNotNull(isSegmentCacheConfigured);
    Assert.assertTrue(isSegmentCacheConfigured);
  }

  @Test
  public void testIsSegmentCacheConfiguredWithNoLocationsConfiguredIsInjected()
  {
    mockSegmentCacheNotConfigured();
    Boolean isSegmentCacheConfigured = injector.getInstance(
        Key.get(Boolean.class, Names.named(StorageNodeModule.IS_SEGMENT_CACHE_CONFIGURED))
    );
    Assert.assertNotNull(isSegmentCacheConfigured);
    Assert.assertFalse(isSegmentCacheConfigured);
  }

  @Test
  public void getDataNodeServiceWithNoServerTypeConfigShouldThrowProvisionException()
  {
    exceptionRule.expect(ProvisionException.class);
    exceptionRule.expectMessage("Must override the binding for ServerTypeConfig if you want a DataNodeService.");
    injector = makeInjector(!INJECT_SERVER_TYPE_CONFIG);
    injector.getInstance(DataNodeService.class);
  }

  @Test
  public void getDataNodeServiceWithNoSegmentCacheConfiguredThrowProvisionException()
  {
    exceptionRule.expect(ProvisionException.class);
    exceptionRule.expectMessage("Segment cache locations must be set on historicals.");
    Mockito.doReturn(ServerType.HISTORICAL).when(serverTypeConfig).getServerType();
    mockSegmentCacheNotConfigured();
    injector.getInstance(DataNodeService.class);
  }

  @Test
  public void getDataNodeServiceIsInjectedAsSingleton()
  {
    DataNodeService dataNodeService = injector.getInstance(DataNodeService.class);
    Assert.assertNotNull(dataNodeService);
    DataNodeService other = injector.getInstance(DataNodeService.class);
    Assert.assertSame(dataNodeService, other);
  }

  @Test
  public void getDataNodeServiceIsInjectedAndDiscoverable()
  {
    DataNodeService dataNodeService = injector.getInstance(DataNodeService.class);
    Assert.assertNotNull(dataNodeService);
    Assert.assertTrue(dataNodeService.isDiscoverable());
  }

  @Test
  public void getDataNodeServiceWithSegmentCacheNotConfiguredIsInjectedAndDiscoverable()
  {
    mockSegmentCacheNotConfigured();
    DataNodeService dataNodeService = injector.getInstance(DataNodeService.class);
    Assert.assertNotNull(dataNodeService);
    Assert.assertFalse(dataNodeService.isDiscoverable());
  }

  @Test
  public void testDruidServerMetadataIsInjectedAsSingleton()
  {
    DruidServerMetadata druidServerMetadata = injector.getInstance(DruidServerMetadata.class);
    Assert.assertNotNull(druidServerMetadata);
    DruidServerMetadata other = injector.getInstance(DruidServerMetadata.class);
    Assert.assertSame(druidServerMetadata, other);
  }

  @Test
  public void testDruidServerMetadataWithNoServerTypeConfigShouldThrowProvisionException()
  {
    exceptionRule.expect(ProvisionException.class);
    exceptionRule.expectMessage("Must override the binding for ServerTypeConfig if you want a DruidServerMetadata.");
    injector = makeInjector(!INJECT_SERVER_TYPE_CONFIG);
    injector.getInstance(DruidServerMetadata.class);
  }

  private Injector makeInjector(boolean withServerTypeConfig)
  {
    return Guice.createInjector(
        Modules.override(
            (binder) -> {
              binder.bind(DruidNode.class).annotatedWith(Self.class).toInstance(self);
              binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
              binder.bind(DruidProcessingConfig.class).toInstance(druidProcessingConfig);
              binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
            },
            target
        ).with(
            (binder) -> {
              binder.bind(SegmentLoaderConfig.class).toInstance(segmentLoaderConfig);
              if (withServerTypeConfig) {
                binder.bind(ServerTypeConfig.class).toInstance(serverTypeConfig);
              }
            }
        )
    );
  }

  private void mockSegmentCacheNotConfigured()
  {
    Mockito.doReturn(Collections.emptyList()).when(segmentLoaderConfig).getLocations();
  }
}
