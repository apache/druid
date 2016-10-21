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

package io.druid.initialization;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;

import io.druid.guice.ExtensionsConfig;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.java.util.common.ISE;
import io.druid.server.DruidNode;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.MethodSorters;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class InitializationTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void test01InitialModulesEmpty() throws Exception
  {
    Initialization.clearLoadedModules();
    Assert.assertEquals(
        "Initial set of loaded modules must be empty",
        0,
        Initialization.getLoadedModules(DruidModule.class).size()
    );
  }

  @Test
  public void test02MakeStartupInjector() throws Exception
  {
    Injector startupInjector = GuiceInjectors.makeStartupInjector();
    Assert.assertNotNull(startupInjector);
    Assert.assertNotNull(startupInjector.getInstance(ObjectMapper.class));
  }

  @Test
  public void test03ClassLoaderExtensionsLoading()
  {
    Injector startupInjector = GuiceInjectors.makeStartupInjector();

    Function<DruidModule, String> fnClassName = new Function<DruidModule, String>()
    {
      @Nullable
      @Override
      public String apply(@Nullable DruidModule input)
      {
        return input.getClass().getCanonicalName();
      }
    };

    Assert.assertFalse(
        "modules does not contain TestDruidModule",
        Collections2.transform(Initialization.getLoadedModules(DruidModule.class), fnClassName)
                    .contains("io.druid.initialization.InitializationTest.TestDruidModule")
    );

    Collection<DruidModule> modules = Initialization.getFromExtensions(
        startupInjector.getInstance(ExtensionsConfig.class),
        DruidModule.class
    );

    Assert.assertTrue(
        "modules contains TestDruidModule",
        Collections2.transform(modules, fnClassName)
                    .contains("io.druid.initialization.InitializationTest.TestDruidModule")
    );
  }

  @Test
  public void test04DuplicateClassLoaderExtensions() throws Exception
  {
    final File extensionDir = temporaryFolder.newFolder();
    Initialization.getLoadersMap().put(extensionDir, (URLClassLoader) Initialization.class.getClassLoader());

    Collection<DruidModule> modules = Initialization.getFromExtensions(new ExtensionsConfig(), DruidModule.class);

    Set<String> loadedModuleNames = Sets.newHashSet();
    for (DruidModule module : modules) {
      Assert.assertFalse("Duplicate extensions are loaded", loadedModuleNames.contains(module.getClass().getName()));
      loadedModuleNames.add(module.getClass().getName());
    }

    Initialization.getLoadersMap().clear();
  }

  @Test
  public void test05MakeInjectorWithModules() throws Exception
  {
    Injector startupInjector = GuiceInjectors.makeStartupInjector();
    Injector injector = Initialization.makeInjectorWithModules(
        startupInjector, ImmutableList.<com.google.inject.Module>of(
            new com.google.inject.Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder, Key.get(DruidNode.class, Self.class), new DruidNode("test-inject", null, null)
                );
              }
            }
        )
    );
    Assert.assertNotNull(injector);
  }

  @Test
  public void test06GetClassLoaderForExtension() throws IOException
  {
    final File some_extension_dir = temporaryFolder.newFolder();
    final File a_jar = new File(some_extension_dir, "a.jar");
    final File b_jar = new File(some_extension_dir, "b.jar");
    final File c_jar = new File(some_extension_dir, "c.jar");
    a_jar.createNewFile();
    b_jar.createNewFile();
    c_jar.createNewFile();
    final URLClassLoader loader = Initialization.getClassLoaderForExtension(some_extension_dir);
    final URL[] expectedURLs = new URL[]{a_jar.toURI().toURL(), b_jar.toURI().toURL(), c_jar.toURI().toURL()};
    final URL[] actualURLs = loader.getURLs();
    Arrays.sort(
        actualURLs, new Comparator<URL>()
        {
          @Override
          public int compare(URL o1, URL o2)
          {
            return o1.getPath().compareTo(o2.getPath());
          }
        }
    );
    Assert.assertArrayEquals(expectedURLs, actualURLs);
  }

  @Test
  public void testGetLoadedModules()
  {

    Set<DruidModule> modules = Initialization.getLoadedModules(DruidModule.class);

    Set<DruidModule> loadedModules = Initialization.getLoadedModules(DruidModule.class);
    Assert.assertEquals("Set from loaded modules #1 should be same!", modules, loadedModules);

    Set<DruidModule> loadedModules2 = Initialization.getLoadedModules(DruidModule.class);
    Assert.assertEquals("Set from loaded modules #2 should be same!", modules, loadedModules2);
  }

  @Test
  public void testGetExtensionFilesToLoad_non_exist_extensions_dir() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder();
    Assert.assertTrue("could not create missing folder", !tmpDir.exists() || tmpDir.delete());
    Assert.assertArrayEquals(
        "Non-exist root extensionsDir should return an empty array of File",
        new File[]{},
        Initialization.getExtensionFilesToLoad(new ExtensionsConfig()
        {
          @Override
          public String getDirectory()
          {
            return tmpDir.getAbsolutePath();
          }
        })
    );
  }

  @Test(expected = ISE.class)
  public void testGetExtensionFilesToLoad_wrong_type_extensions_dir() throws IOException
  {
    final File extensionsDir = temporaryFolder.newFile();
    final ExtensionsConfig config = new ExtensionsConfig()
    {
      @Override
      public String getDirectory()
      {
        return extensionsDir.getAbsolutePath();
      }
    };
    Initialization.getExtensionFilesToLoad(config);
  }

  @Test
  public void testGetExtensionFilesToLoad_empty_extensions_dir() throws IOException
  {
    final File extensionsDir = temporaryFolder.newFolder();
    final ExtensionsConfig config = new ExtensionsConfig()
    {
      @Override
      public String getDirectory()
      {
        return extensionsDir.getAbsolutePath();
      }
    };

    Assert.assertArrayEquals(
        "Empty root extensionsDir should return an empty array of File",
        new File[]{},
        Initialization.getExtensionFilesToLoad(config)
    );
  }

  /**
   * If druid.extension.load is not specified, Initialization.getExtensionFilesToLoad is supposed to return all the
   * extension folders under root extensions directory.
   */
  @Test
  public void testGetExtensionFilesToLoad_null_load_list() throws IOException
  {
    final File extensionsDir = temporaryFolder.newFolder();
    final ExtensionsConfig config = new ExtensionsConfig()
    {
      @Override
      public String getDirectory()
      {
        return extensionsDir.getAbsolutePath();
      }
    };
    final File mysql_metadata_storage = new File(extensionsDir, "mysql-metadata-storage");
    final File druid_kafka_eight = new File(extensionsDir, "druid-kafka-eight");
    mysql_metadata_storage.mkdir();
    druid_kafka_eight.mkdir();

    final File[] expectedFileList = new File[]{druid_kafka_eight, mysql_metadata_storage};
    final File[] actualFileList = Initialization.getExtensionFilesToLoad(config);
    Arrays.sort(actualFileList);
    Assert.assertArrayEquals(expectedFileList, actualFileList);
  }

  /**
   * druid.extension.load is specified, Initialization.getExtensionFilesToLoad is supposed to return all the extension
   * folders appeared in the load list.
   */
  @Test
  public void testGetExtensionFilesToLoad_with_load_list() throws IOException
  {
    final File extensionsDir = temporaryFolder.newFolder();
    final ExtensionsConfig config = new ExtensionsConfig()
    {
      @Override
      public List<String> getLoadList()
      {
        return Arrays.asList("mysql-metadata-storage", "druid-kafka-eight");
      }

      @Override
      public String getDirectory()
      {
        return extensionsDir.getAbsolutePath();
      }
    };
    final File mysql_metadata_storage = new File(extensionsDir, "mysql-metadata-storage");
    final File druid_kafka_eight = new File(extensionsDir, "druid-kafka-eight");
    final File random_extension = new File(extensionsDir, "random-extensions");
    mysql_metadata_storage.mkdir();
    druid_kafka_eight.mkdir();
    random_extension.mkdir();

    final File[] expectedFileList = new File[]{druid_kafka_eight, mysql_metadata_storage};
    final File[] actualFileList = Initialization.getExtensionFilesToLoad(config);
    Arrays.sort(actualFileList);
    Assert.assertArrayEquals(expectedFileList, actualFileList);
  }

  /**
   * druid.extension.load is specified, but contains an extension that is not prepared under root extension directory.
   * Initialization.getExtensionFilesToLoad is supposed to throw ISE.
   */
  @Test(expected = ISE.class)
  public void testGetExtensionFilesToLoad_with_non_exist_item_in_load_list() throws IOException
  {
    final File extensionsDir = temporaryFolder.newFolder();
    final ExtensionsConfig config = new ExtensionsConfig()
    {
      @Override
      public List<String> getLoadList()
      {
        return Arrays.asList("mysql-metadata-storage", "druid-kafka-eight");
      }

      @Override
      public String getDirectory()
      {
        return extensionsDir.getAbsolutePath();
      }
    };
    final File druid_kafka_eight = new File(extensionsDir, "druid-kafka-eight");
    final File random_extension = new File(extensionsDir, "random-extensions");
    druid_kafka_eight.mkdir();
    random_extension.mkdir();
    Initialization.getExtensionFilesToLoad(config);
  }

  @Test(expected = ISE.class)
  public void testGetHadoopDependencyFilesToLoad_wrong_type_root_hadoop_depenencies_dir() throws IOException
  {
    final File rootHadoopDependenciesDir = temporaryFolder.newFile();
    final ExtensionsConfig config = new ExtensionsConfig()
    {
      @Override
      public String getHadoopDependenciesDir()
      {
        return rootHadoopDependenciesDir.getAbsolutePath();
      }
    };
    Initialization.getHadoopDependencyFilesToLoad(ImmutableList.<String>of(), config);
  }

  @Test(expected = ISE.class)
  public void testGetHadoopDependencyFilesToLoad_non_exist_version_dir() throws IOException
  {
    final File rootHadoopDependenciesDir = temporaryFolder.newFolder();
    final ExtensionsConfig config = new ExtensionsConfig()
    {
      @Override
      public String getHadoopDependenciesDir()
      {
        return rootHadoopDependenciesDir.getAbsolutePath();
      }
    };
    final File hadoopClient = new File(rootHadoopDependenciesDir, "hadoop-client");
    hadoopClient.mkdir();
    Initialization.getHadoopDependencyFilesToLoad(ImmutableList.of("org.apache.hadoop:hadoop-client:2.3.0"), config);
  }

  @Test
  public void testGetHadoopDependencyFilesToLoad_with_hadoop_coordinates() throws IOException
  {
    final File rootHadoopDependenciesDir = temporaryFolder.newFolder();
    final ExtensionsConfig config = new ExtensionsConfig()
    {
      @Override
      public String getHadoopDependenciesDir()
      {
        return rootHadoopDependenciesDir.getAbsolutePath();
      }
    };
    final File hadoopClient = new File(rootHadoopDependenciesDir, "hadoop-client");
    final File versionDir = new File(hadoopClient, "2.3.0");
    hadoopClient.mkdir();
    versionDir.mkdir();
    final File[] expectedFileList = new File[]{versionDir};
    final File[] actualFileList = Initialization.getHadoopDependencyFilesToLoad(
        ImmutableList.of(
            "org.apache.hadoop:hadoop-client:2.3.0"
        ), config
    );
    Assert.assertArrayEquals(expectedFileList, actualFileList);
  }

  @Test
  public void testGetURLsForClasspath() throws Exception
  {
    File tmpDir1 = temporaryFolder.newFolder();
    File tmpDir2 = temporaryFolder.newFolder();
    File tmpDir3 = temporaryFolder.newFolder();

    File tmpDir1a = new File(tmpDir1, "a.jar");
    tmpDir1a.createNewFile();
    File tmpDir1b = new File(tmpDir1, "b.jar");
    tmpDir1b.createNewFile();
    new File(tmpDir1, "note1.txt").createNewFile();

    File tmpDir2c = new File(tmpDir2, "c.jar");
    tmpDir2c.createNewFile();
    File tmpDir2d = new File(tmpDir2, "d.jar");
    tmpDir2d.createNewFile();
    File tmpDir2e = new File(tmpDir2, "e.JAR");
    tmpDir2e.createNewFile();
    new File(tmpDir2, "note2.txt").createNewFile();

    String cp = tmpDir1.getAbsolutePath() + File.separator + "*"
                + File.pathSeparator
                + tmpDir3.getAbsolutePath()
                + File.pathSeparator
                + tmpDir2.getAbsolutePath() + File.separator + "*";

    // getURLsForClasspath uses listFiles which does NOT guarantee any ordering for the name strings.
    List<URL> urLsForClasspath = Initialization.getURLsForClasspath(cp);
    Assert.assertEquals(Sets.newHashSet(tmpDir1a.toURI().toURL(), tmpDir1b.toURI().toURL()),
                        Sets.newHashSet(urLsForClasspath.subList(0, 2)));
    Assert.assertEquals(tmpDir3.toURI().toURL(), urLsForClasspath.get(2));
    Assert.assertEquals(Sets.newHashSet(tmpDir2c.toURI().toURL(), tmpDir2d.toURI().toURL(), tmpDir2e.toURI().toURL()),
                        Sets.newHashSet(urLsForClasspath.subList(3, 6)));


  }

  @Test
  public void testExtensionsWithSameDirName() throws Exception
  {
    final String extensionName = "some_extension";
    final File tmpDir1 = temporaryFolder.newFolder();
    final File tmpDir2 = temporaryFolder.newFolder();
    final File extension1 = new File(tmpDir1, extensionName);
    final File extension2 = new File(tmpDir2, extensionName);
    Assert.assertTrue(extension1.mkdir());
    Assert.assertTrue(extension2.mkdir());
    final File jar1 = new File(extension1, "jar1.jar");
    final File jar2 = new File(extension2, "jar2.jar");

    Assert.assertTrue(jar1.createNewFile());
    Assert.assertTrue(jar2.createNewFile());

    final ClassLoader classLoader1 = Initialization.getClassLoaderForExtension(extension1);
    final ClassLoader classLoader2 = Initialization.getClassLoaderForExtension(extension2);

    Assert.assertArrayEquals(new URL[]{jar1.toURL()}, ((URLClassLoader) classLoader1).getURLs());
    Assert.assertArrayEquals(new URL[]{jar2.toURL()}, ((URLClassLoader) classLoader2).getURLs());
  }

  public static class TestDruidModule implements DruidModule
  {
    @Override
    public List<? extends Module> getJacksonModules()
    {
      return ImmutableList.of();
    }

    @Override
    public void configure(Binder binder)
    {
      // Do nothing
    }
  }
}
