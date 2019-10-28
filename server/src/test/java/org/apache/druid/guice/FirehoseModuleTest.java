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

import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.reflect.ClassPath;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.segment.realtime.firehose.ClippedFirehoseFactory;
import org.apache.druid.utils.JvmUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class FirehoseModuleTest
{
  private static final Predicate<Class> IS_FIREHOSE_FACTORY =
      c -> FirehoseFactory.class.isAssignableFrom(c) && !Modifier.isAbstract(c.getModifiers());

  @Test
  public void testAllFirehoseFactorySubtypesRegistered() throws IOException
  {
    ObjectMapper objectMapper = createObjectMapper();
    Set<Class> registeredSubtypeClasses = getFirehoseFactorySubtypeClasses(objectMapper);
    String packageName = ClippedFirehoseFactory.class.getPackage().getName();
    Set<Class> expectedSubtypeClasses = getFirehoseFactoryClassesInPackage(packageName);
    Assert.assertEquals(expectedSubtypeClasses, registeredSubtypeClasses);
  }

  private static ObjectMapper createObjectMapper()
  {
    ObjectMapper objectMapper = new ObjectMapper();
    for (Module jacksonModule : new FirehoseModule().getJacksonModules()) {
      objectMapper.registerModule(jacksonModule);
    }
    return objectMapper;
  }

  private static Set<Class> getFirehoseFactorySubtypeClasses(ObjectMapper objectMapper)
  {
    Class parentClass = FirehoseFactory.class;
    MapperConfig config = objectMapper.getDeserializationConfig();
    AnnotationIntrospector annotationIntrospector = config.getAnnotationIntrospector();
    AnnotatedClass ac = AnnotatedClass.constructWithoutSuperTypes(parentClass, annotationIntrospector, config);
    Collection<NamedType> subtypes = objectMapper.getSubtypeResolver().collectAndResolveSubtypesByClass(config, ac);
    Assert.assertNotNull(subtypes);
    return subtypes.stream()
                   .map(NamedType::getType)
                   .filter(c -> !c.equals(parentClass))
                   .collect(Collectors.toSet());
  }

  @SuppressWarnings("UnstableApiUsage") // for ClassPath
  private static Set<Class> getFirehoseFactoryClassesInPackage(String packageName) throws IOException
  {
    // workaround for Guava 16, which can only parse the classpath from URLClassLoaders
    // requires Guava 28 or later to work properly with the system class loader in Java 9 and above
    URLClassLoader classloader = new URLClassLoader(JvmUtils.systemClassPath().toArray(new URL[0]));
    ClassPath classPath = ClassPath.from(classloader);
    return classPath.getTopLevelClasses(packageName).stream()
                    .map(ClassPath.ClassInfo::load)
                    .filter(IS_FIREHOSE_FACTORY)
                    .collect(Collectors.toSet());
  }
}

