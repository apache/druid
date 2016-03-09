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

package io.druid.indexing.common.task;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.metamx.common.logger.Logger;
import io.druid.guice.ExtensionsConfig;
import io.druid.guice.GuiceInjectors;
import io.druid.indexing.common.TaskToolbox;
import io.druid.initialization.Initialization;

import javax.annotation.Nullable;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public abstract class HadoopTask extends AbstractTask
{
  private static final Logger log = new Logger(HadoopTask.class);
  private static final ExtensionsConfig extensionsConfig;

  final static Injector injector = GuiceInjectors.makeStartupInjector();

  static {
    extensionsConfig = injector.getInstance(ExtensionsConfig.class);
  }

  private final List<String> hadoopDependencyCoordinates;

  protected HadoopTask(
      String id,
      String dataSource,
      List<String> hadoopDependencyCoordinates,
      Map<String, Object> context
  )
  {
    super(id, dataSource, context);
    this.hadoopDependencyCoordinates = hadoopDependencyCoordinates;
  }

  public List<String> getHadoopDependencyCoordinates()
  {
    return hadoopDependencyCoordinates == null ? null : ImmutableList.copyOf(hadoopDependencyCoordinates);
  }

  // This could stand to have a more robust detection methodology.
  // Right now it just looks for `druid.*\.jar`
  // This is only used for classpath isolation in the runTask isolation stuff, so it shooouuullldddd be ok.
  protected static final Predicate<URL> IS_DRUID_URL = new Predicate<URL>()
  {
    @Override
    public boolean apply(@Nullable URL input)
    {
      try {
        if (input == null) {
          return false;
        }
        final String fName = Paths.get(input.toURI()).getFileName().toString();
        return fName.startsWith("druid") && fName.endsWith(".jar") && !fName.endsWith("selfcontained.jar");
      }
      catch (URISyntaxException e) {
        throw Throwables.propagate(e);
      }
    }
  };

  protected ClassLoader buildClassLoader(final TaskToolbox toolbox) throws Exception
  {
    final List<String> finalHadoopDependencyCoordinates = hadoopDependencyCoordinates != null
                                                          ? hadoopDependencyCoordinates
                                                          : toolbox.getConfig().getDefaultHadoopCoordinates();

    final List<URL> jobURLs = Lists.newArrayList(
        Arrays.asList(((URLClassLoader) HadoopIndexTask.class.getClassLoader()).getURLs())
    );

    for (final File extension : Initialization.getExtensionFilesToLoad(extensionsConfig)) {
      final ClassLoader extensionLoader = Initialization.getClassLoaderForExtension(extension);
      jobURLs.addAll(Arrays.asList(((URLClassLoader) extensionLoader).getURLs()));
    }

    final List<URL> localClassLoaderURLs = new ArrayList<>(jobURLs);

    // hadoop dependencies come before druid classes because some extensions depend on them
    for (final File hadoopDependency :
        Initialization.getHadoopDependencyFilesToLoad(
            finalHadoopDependencyCoordinates,
            extensionsConfig
        )) {
      final ClassLoader hadoopLoader = Initialization.getClassLoaderForExtension(hadoopDependency);
      localClassLoaderURLs.addAll(Arrays.asList(((URLClassLoader) hadoopLoader).getURLs()));
    }

    final ClassLoader classLoader = new URLClassLoader(
        localClassLoaderURLs.toArray(new URL[localClassLoaderURLs.size()]),
        null
    );

    System.setProperty("druid.hadoop.internal.classpath", Joiner.on(File.pathSeparator).join(jobURLs));
    return classLoader;
  }

  /**
   * This method tries to isolate class loading during a Function call
   *
   * @param clazzName    The Class which has a static method called `runTask`
   * @param input        The input for `runTask`, must have `input.getClass()` be the class of the input for runTask
   * @param loader       The loader to use as the context class loader during invocation
   * @param <InputType>  The input type of the method.
   * @param <OutputType> The output type of the method. The result of runTask must be castable to this type.
   *
   * @return The result of the method invocation
   */
  public static <InputType, OutputType> OutputType invokeForeignLoader(
      final String clazzName,
      final InputType input,
      final ClassLoader loader
  )
  {
    log.debug("Launching [%s] on class loader [%s] with input class [%s]", clazzName, loader, input.getClass());
    final ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(loader);
      final Class<?> clazz = loader.loadClass(clazzName);
      final Method method = clazz.getMethod("runTask", input.getClass());
      return (OutputType) method.invoke(null, input);
    }
    catch (IllegalAccessException | InvocationTargetException | ClassNotFoundException | NoSuchMethodException e) {
      throw Throwables.propagate(e);
    }
    finally {
      Thread.currentThread().setContextClassLoader(oldLoader);
    }
  }
}
