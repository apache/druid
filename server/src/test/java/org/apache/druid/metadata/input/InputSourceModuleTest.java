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

package org.apache.druid.metadata.input;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.AnnotatedClassResolver;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class InputSourceModuleTest
{
  private final ObjectMapper mapper = new ObjectMapper();
  private final String SQL_NAMED_TYPE = "sql";

  @Before
  public void setUp()
  {
    for (Module jacksonModule : new InputSourceModule().getJacksonModules()) {
      mapper.registerModule(jacksonModule);
    }
  }

  @Test
  public void testSubTypeRegistration()
  {
    MapperConfig config = mapper.getDeserializationConfig();
    AnnotatedClass annotatedClass = AnnotatedClassResolver.resolveWithoutSuperTypes(config, SqlInputSource.class);
    List<String> subtypes = mapper.getSubtypeResolver()
                                  .collectAndResolveSubtypesByClass(config, annotatedClass)
                                  .stream()
                                  .map(NamedType::getName)
                                  .collect(Collectors.toList());
    Assert.assertNotNull(subtypes);
    Assert.assertEquals(SQL_NAMED_TYPE, Iterables.getOnlyElement(subtypes));
  }
}
