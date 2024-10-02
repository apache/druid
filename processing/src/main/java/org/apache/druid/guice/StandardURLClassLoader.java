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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;


/**
 * The ClassLoader that gets used when druid.extensions.useExtensionClassloaderFirst = false.
 */
public class StandardURLClassLoader extends URLClassLoader
{
  private final List<ClassLoader> extensionDependencyClassLoaders;

  public StandardURLClassLoader(final URL[] urls, final ClassLoader druidLoader, final List<ClassLoader> extensionDependencyClassLoaders)
  {
    super(urls, druidLoader);
    this.extensionDependencyClassLoaders = Preconditions.checkNotNull(extensionDependencyClassLoaders, "extensionDependencyClassLoaders");
  }

  @Override
  protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException
  {
    Class<?> clazz;
    try {
      clazz = super.loadClass(name, resolve);
    }
    catch (ClassNotFoundException e) {
      clazz = loadClassFromExtensionDependencies(name);
    }
    if (resolve) {
      resolveClass(clazz);
    }

    return clazz;
  }

  @Override
  public URL getResource(final String name)
  {
    URL resource = super.getResource(name);

    if (resource != null) {
      return resource;
    }

    return getResourceFromExtensionsDependencies(name);
  }

  @Override
  public Enumeration<URL> getResources(final String name) throws IOException
  {
    final List<URL> urls = new ArrayList<>();
    Iterators.addAll(urls, Iterators.forEnumeration(super.getResources(name)));
    addExtensionResources(name, urls);
    return Iterators.asEnumeration(urls.iterator());
  }

  protected URL getResourceFromExtensionsDependencies(final String name)
  {
    URL resourceFromExtension = null;
    for (ClassLoader classLoader : extensionDependencyClassLoaders) {
      resourceFromExtension = classLoader.getResource(name);
      if (resourceFromExtension != null) {
        break;
      }
    }
    return resourceFromExtension;
  }

  protected Class<?> loadClassFromExtensionDependencies(final String name) throws ClassNotFoundException
  {
    for (ClassLoader classLoader : extensionDependencyClassLoaders) {
      try {
        return classLoader.loadClass(name);
      }
      catch (ClassNotFoundException ignored) {
      }
    }
    throw new ClassNotFoundException();
  }

  protected void addExtensionResources(final String name, List<URL> urls) throws IOException
  {
    for (ClassLoader classLoader : extensionDependencyClassLoaders) {
      Iterators.addAll(urls, Iterators.forEnumeration(classLoader.getResources(name)));
    }
  }

  public List<ClassLoader> getExtensionDependencyClassLoaders()
  {
    return extensionDependencyClassLoaders;
  }
}
