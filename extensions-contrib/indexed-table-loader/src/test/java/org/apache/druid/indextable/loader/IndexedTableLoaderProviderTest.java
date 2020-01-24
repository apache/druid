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

package org.apache.druid.indextable.loader;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indextable.loader.config.IndexedTableConfig;
import org.apache.druid.indextable.loader.config.IndexedTableLoaderConfig;
import org.apache.druid.java.util.common.ISE;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class IndexedTableLoaderProviderTest
{
  private static final String CONFIG_FILE_PATH = "CONFIG_FILE_PATH";

  @Mock
  private ObjectMapper mapper;
  @Mock
  private ObjectReader reader;
  @Mock
  private IndexedTableLoaderConfig config;
  @Mock
  private File configFile;
  @Mock
  private Map<String, IndexedTableConfig> loadMap;

  private IndexedTableLoaderProvider target;

  @Before
  public void setUp() throws IOException
  {
    Mockito.when(config.getConfigFilePath()).thenReturn(CONFIG_FILE_PATH);
    Mockito.when(mapper.readerFor(ArgumentMatchers.<TypeReference<ImmutableMap<String, IndexedTableConfig>>>any()))
           .thenReturn(reader);
    Mockito.when(reader.readValue(ArgumentMatchers.any(File.class))).thenReturn(loadMap);

    target = Mockito.spy(new IndexedTableLoaderProvider(config, mapper));
    Mockito.doReturn(configFile).when(target).getFile(CONFIG_FILE_PATH);
  }

  @Test
  public void testGetShouldLoadFileSuccessfully()
  {
    Map<String, IndexedTableConfig> loadMap = target.get();
    Assert.assertEquals(this.loadMap, loadMap);
  }

  @Test(expected = ISE.class)
  public void testGetCanNotReadFileShouldThrowISE() throws IOException
  {
    Mockito.doThrow(IOException.class).when(reader).readValue(configFile);
    Map<String, IndexedTableConfig> loadMap = target.get();
    Assert.assertEquals(this.loadMap, loadMap);
  }
}
