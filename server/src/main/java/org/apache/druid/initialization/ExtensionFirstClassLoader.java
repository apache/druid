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

package org.apache.druid.initialization;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * The ClassLoader that gets used when druid.extensions.useExtensionClassloaderFirst = true.
 */
public class ExtensionFirstClassLoader extends URLClassLoader
{
  private final ClassLoader druidLoader;

  public ExtensionFirstClassLoader(final URL[] urls, final ClassLoader druidLoader)
  {
    super(urls, null);
    this.druidLoader = Preconditions.checkNotNull(druidLoader, "druidLoader");
  }

  @Override
  public Class<?> loadClass(final String name) throws ClassNotFoundException
  {
    return loadClass(name, false);
  }

  @Override
  protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException
  {
    synchronized (getClassLoadingLock(name)) {
      Class<?> clazz = findLoadedClass(name);

      if (clazz == null) {
        // Try extension classloader first.
        try {
          clazz = findClass(name);
        }
        catch (ClassNotFoundException e) {
          // Try the Druid classloader. Will throw ClassNotFoundException if the class can't be loaded.
          return druidLoader.loadClass(name);
        }
      }

      if (resolve) {
        resolveClass(clazz);
      }

      return clazz;
    }
  }

  @Override
  public URL getResource(final String name)
  {
    final URL resourceFromExtension = super.getResource(name);

    if (resourceFromExtension != null) {
      return resourceFromExtension;
    } else {
      return druidLoader.getResource(name);
    }
  }

  @Override
  public Enumeration<URL> getResources(final String name) throws IOException
  {
    final List<URL> urls = new ArrayList<>();
    Iterators.addAll(urls, Iterators.forEnumeration(super.getResources(name)));
    Iterators.addAll(urls, Iterators.forEnumeration(druidLoader.getResources(name)));
    return Iterators.asEnumeration(urls.iterator());
  }
}
