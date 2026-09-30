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

package org.apache.druid.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.jar.JarFile;

public class ConnectionUriUtilsMariaDb2xTest
{
  private static final String MYSQL_URI =
      "jdbc:mysql://localhost:3306/test?user=druid&password=diurd&keyonly&otherOptions=wat";
  private static final String MARIA_URI =
      "jdbc:mariadb://localhost:3306/test?user=druid&password=diurd&keyonly&otherOptions=wat";
  private static final Set<String> EXPECTED_PROPERTIES = Set.of("user", "password", "keyonly", "otherOptions");

  @Test
  public void testMariaDb2xDriverUriDispatch() throws Exception
  {
    final Path driverJar = Path.of(System.getProperty("druid.test.mariadbLegacyDriver"));
    Assertions.assertTrue(Files.isRegularFile(driverJar), "MariaDB Connector/J 2.x test driver is missing");

    try (final JarFile jarFile = new JarFile(driverJar.toFile())) {
      Assertions.assertEquals(
          "2.7.3",
          jarFile.getManifest().getMainAttributes().getValue("Bundle-Version")
      );
    }

    final URL processingClasses = ConnectionUriUtils.class.getProtectionDomain().getCodeSource().getLocation();
    try (final MariaDb2xClassLoader classLoader = new MariaDb2xClassLoader(
        new URL[]{processingClasses, driverJar.toUri().toURL()},
        ConnectionUriUtils.class.getClassLoader()
    )) {
      final Class<?> isolatedUtils = classLoader.loadClass(ConnectionUriUtils.class.getName());
      final Method directParser = isolatedUtils.getMethod("tryParseMariaDb2xConnectionUri", String.class);
      final Method dispatcher = isolatedUtils.getMethod("tryParseJdbcUriParameters", String.class, boolean.class);

      for (final String uri : new String[]{MYSQL_URI, MARIA_URI}) {
        Assertions.assertEquals(EXPECTED_PROPERTIES, invoke(directParser, uri));
        Assertions.assertEquals(EXPECTED_PROPERTIES, invoke(dispatcher, uri, false));
      }

      Assertions.assertThrows(
          ClassNotFoundException.class,
          () -> ConnectionUriUtils.class.getClassLoader().loadClass("org.mariadb.jdbc.UrlParser")
      );
      final Class<?> urlParser = classLoader.loadClass("org.mariadb.jdbc.UrlParser");
      Assertions.assertSame(classLoader, urlParser.getClassLoader());
      Assertions.assertNotSame(
          classLoader,
          Class.forName("org.mariadb.jdbc.Configuration").getClassLoader()
      );
      Assertions.assertThrows(
          ClassNotFoundException.class,
          () -> classLoader.loadClass("org.mariadb.jdbc.Configuration")
      );
      Assertions.assertEquals(
          driverJar.toRealPath(),
          Path.of(urlParser.getProtectionDomain().getCodeSource().getLocation().toURI()).toRealPath()
      );
    }
  }

  @SuppressWarnings("unchecked")
  private static Set<String> invoke(final Method method, final Object... arguments) throws Exception
  {
    return (Set<String>) method.invoke(null, arguments);
  }

  private static final class MariaDb2xClassLoader extends URLClassLoader
  {
    private MariaDb2xClassLoader(final URL[] urls, final ClassLoader parent)
    {
      super(urls, parent);
    }

    @Override
    protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException
    {
      synchronized (getClassLoadingLock(name)) {
        Class<?> loadedClass = findLoadedClass(name);
        if (loadedClass == null) {
          if (name.equals(ConnectionUriUtils.class.getName()) || name.startsWith("org.mariadb.jdbc.")) {
            loadedClass = findClass(name);
          } else if (name.startsWith("com.mysql.cj.")) {
            throw new ClassNotFoundException(name);
          } else {
            loadedClass = super.loadClass(name, false);
          }
        }
        if (resolve) {
          resolveClass(loadedClass);
        }
        return loadedClass;
      }
    }
  }
}
