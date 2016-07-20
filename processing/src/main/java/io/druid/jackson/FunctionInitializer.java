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

package io.druid.jackson;

import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.metamx.common.logger.Logger;
import io.druid.math.expr.Function;
import io.druid.math.expr.Parser;

import java.util.List;

/**
 */
public class FunctionInitializer implements Module
{
  private static final Logger log = new Logger(FunctionInitializer.class);

  @Inject
  public static void init(ObjectMapper mapper)
  {
    log.info("finding expression functions..");
    for (NamedType subType : resolveSubtypes(mapper, Function.Library.class)) {
      Parser.register(subType.getType());
    }
  }

  private static List<NamedType> resolveSubtypes(ObjectMapper mapper, Class<?> clazz)
  {
    JavaType type = mapper.getTypeFactory().constructType(clazz);

    DeserializationConfig config = mapper.getDeserializationConfig();
    AnnotatedClass annotated = config.introspectClassAnnotations(type).getClassInfo();
    AnnotationIntrospector inspector = config.getAnnotationIntrospector();

    SubtypeResolver resolver = mapper.getSubtypeResolver();

    List<NamedType> found = Lists.newArrayList();
    for (NamedType resolved : resolver.collectAndResolveSubtypes(annotated, config, inspector)) {
      if (resolved.getType() != clazz) {  // filter self
        found.add(resolved);
      }
    }
    return found;
  }

  @Override
  public void configure(Binder binder)
  {
    binder.requestStaticInjection(FunctionInitializer.class);
  }
}
