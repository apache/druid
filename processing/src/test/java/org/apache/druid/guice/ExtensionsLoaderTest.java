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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * Some of the tests in this class rely on the file
 * {@code src/test/resources/META-INF/services/org.apache.druid.initialization.DruidModule}
 * containing an entry for the {@link TestDruidModule}.
 */
public class ExtensionsLoaderTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Map<String, byte[]> jarFileContents = ImmutableMap.of(
      "jar-resource",
      "jar-resource-contents".getBytes(Charset.defaultCharset())
  );

  private Injector startupInjector()
  {
    return new StartupInjectorBuilder()
        .withEmptyProperties()
        .withExtensions()
        .build();
  }

  @Test
  public void test02MakeStartupInjector()
  {
    Injector startupInjector = startupInjector();
    Assert.assertNotNull(startupInjector);
    Assert.assertNotNull(startupInjector.getInstance(ObjectMapper.class));
    ExtensionsLoader extnLoader = ExtensionsLoader.instance(startupInjector);
    Assert.assertNotNull(extnLoader);
    Assert.assertSame(extnLoader, ExtensionsLoader.instance(startupInjector));
  }

  @Test
  public void test04DuplicateClassLoaderExtensions() throws Exception
  {
    final File extensionDir = temporaryFolder.newFolder();
    Injector startupInjector = startupInjector();
    ExtensionsLoader extnLoader = ExtensionsLoader.instance(startupInjector);

    Pair<File, Boolean> key = Pair.of(extensionDir, true);
    extnLoader.getLoadersMap()
                  .put(key, new StandardURLClassLoader(new URL[]{}, ExtensionsLoader.class.getClassLoader(), ImmutableList.of()));

    Collection<DruidModule> modules = extnLoader.getFromExtensions(DruidModule.class);

    Set<String> loadedModuleNames = new HashSet<>();
    for (DruidModule module : modules) {
      Assert.assertFalse("Duplicate extensions are loaded", loadedModuleNames.contains(module.getClass().getName()));
      loadedModuleNames.add(module.getClass().getName());
    }
  }

  @Test
  public void test06GetClassLoaderForExtension() throws IOException
  {
    final ExtensionsLoader extnLoader = new ExtensionsLoader(new ExtensionsConfig(), objectMapper);

    final File some_extension_dir = temporaryFolder.newFolder();
    final File a_jar = new File(some_extension_dir, "a.jar");
    final File b_jar = new File(some_extension_dir, "b.jar");
    final File c_jar = new File(some_extension_dir, "c.jar");
    createNewJar(a_jar, jarFileContents);
    createNewJar(b_jar, jarFileContents);
    createNewJar(c_jar, jarFileContents);


    final StandardURLClassLoader loader = extnLoader.getClassLoaderForExtension(some_extension_dir, false);
    final URL[] expectedURLs = new URL[]{a_jar.toURI().toURL(), b_jar.toURI().toURL(), c_jar.toURI().toURL()};
    final URL[] actualURLs = loader.getURLs();
    Arrays.sort(actualURLs, Comparator.comparing(URL::getPath));
    Assert.assertArrayEquals(expectedURLs, actualURLs);
  }

  @Test
  public void testGetLoadedModules()
  {
    final ExtensionsLoader extnLoader = new ExtensionsLoader(new ExtensionsConfig(), objectMapper);
    Collection<DruidModule> modules = extnLoader.getModules();
    HashSet<DruidModule> moduleSet = new HashSet<>(modules);

    Collection<DruidModule> loadedModules = extnLoader.getModules();
    Assert.assertEquals("Set from loaded modules #1 should be same!", modules.size(), loadedModules.size());
    Assert.assertEquals("Set from loaded modules #1 should be same!", moduleSet, new HashSet<>(loadedModules));

    Collection<DruidModule> loadedModules2 = extnLoader.getModules();
    Assert.assertEquals("Set from loaded modules #2 should be same!", modules.size(), loadedModules2.size());
    Assert.assertEquals("Set from loaded modules #2 should be same!", moduleSet, new HashSet<>(loadedModules2));
  }

  @Test
  public void testGetExtensionFilesToLoad_non_exist_extensions_dir() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder();
    Assert.assertTrue("could not create missing folder", !tmpDir.exists() || tmpDir.delete());
    final ExtensionsLoader extnLoader = new ExtensionsLoader(new ExtensionsConfig()
    {
      @Override
      public String getDirectory()
      {
        return tmpDir.getAbsolutePath();
      }
    }, objectMapper);
    Assert.assertArrayEquals(
        "Non-exist root extensionsDir should return an empty array of File",
        new File[]{},
        extnLoader.getExtensionFilesToLoad()
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
    final ExtensionsLoader extnLoader = new ExtensionsLoader(config, objectMapper);
    extnLoader.getExtensionFilesToLoad();
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

    final ExtensionsLoader extnLoader = new ExtensionsLoader(config, objectMapper);
    Assert.assertArrayEquals(
        "Empty root extensionsDir should return an empty array of File",
        new File[]{},
        extnLoader.getExtensionFilesToLoad()
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
    final ExtensionsLoader extnLoader = new ExtensionsLoader(config, objectMapper);
    final File mysql_metadata_storage = new File(extensionsDir, "mysql-metadata-storage");
    mysql_metadata_storage.mkdir();

    final File[] expectedFileList = new File[]{mysql_metadata_storage};
    final File[] actualFileList = extnLoader.getExtensionFilesToLoad();
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

    final File absolutePathExtension = temporaryFolder.newFolder();

    final ExtensionsConfig config = new ExtensionsConfig()
    {
      @Override
      public LinkedHashSet<String> getLoadList()
      {
        return Sets.newLinkedHashSet(Arrays.asList("mysql-metadata-storage", absolutePathExtension.getAbsolutePath()));
      }

      @Override
      public String getDirectory()
      {
        return extensionsDir.getAbsolutePath();
      }
    };
    final ExtensionsLoader extnLoader = new ExtensionsLoader(config, objectMapper);
    final File mysql_metadata_storage = new File(extensionsDir, "mysql-metadata-storage");
    final File random_extension = new File(extensionsDir, "random-extensions");

    mysql_metadata_storage.mkdir();
    random_extension.mkdir();

    final File[] expectedFileList = new File[]{mysql_metadata_storage, absolutePathExtension};
    final File[] actualFileList = extnLoader.getExtensionFilesToLoad();
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
      public LinkedHashSet<String> getLoadList()
      {
        return Sets.newLinkedHashSet(ImmutableList.of("mysql-metadata-storage"));
      }

      @Override
      public String getDirectory()
      {
        return extensionsDir.getAbsolutePath();
      }
    };
    final File random_extension = new File(extensionsDir, "random-extensions");
    random_extension.mkdir();
    final ExtensionsLoader extnLoader = new ExtensionsLoader(config, objectMapper);
    extnLoader.getExtensionFilesToLoad();
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
    List<URL> urLsForClasspath = ExtensionsLoader.getURLsForClasspath(cp);
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

    createNewJar(jar1, jarFileContents);
    createNewJar(jar2, jarFileContents);

    final ExtensionsLoader extnLoader = new ExtensionsLoader(new ExtensionsConfig(), objectMapper);
    final ClassLoader classLoader1 = extnLoader.getClassLoaderForExtension(extension1, false);
    final ClassLoader classLoader2 = extnLoader.getClassLoaderForExtension(extension2, false);

    Assert.assertArrayEquals(new URL[]{jar1.toURI().toURL()}, ((StandardURLClassLoader) classLoader1).getURLs());
    Assert.assertArrayEquals(new URL[]{jar2.toURI().toURL()}, ((StandardURLClassLoader) classLoader2).getURLs());
  }

  @Test
  public void testGetClassLoaderForExtension_withMissingDependency() throws IOException
  {
    final ExtensionsLoader extnLoader = new ExtensionsLoader(new ExtensionsConfig(), objectMapper);
    final String druidExtensionDependency = "other-druid-extension";
    final DruidExtensionDependencies druidExtensionDependencies = new DruidExtensionDependencies(ImmutableList.of(druidExtensionDependency));

    final File extensionDir = temporaryFolder.newFolder();
    final File extensionJar = new File(extensionDir, "a.jar");
    createNewJar(extensionJar, ImmutableMap.of("druid-extension-dependencies.json", objectMapper.writeValueAsBytes(druidExtensionDependencies)));

    RE exception = Assert.assertThrows(RE.class, () -> {
      extnLoader.getClassLoaderForExtension(extensionDir, false);
    });

    Assert.assertEquals(
        StringUtils.format("Extension [%s] depends on [%s] which is not a valid extension or not loaded.", extensionDir.getName(), druidExtensionDependency),
        exception.getMessage()
    );
  }

  @Test
  public void testGetClassLoaderForExtension_dependencyLoaded() throws IOException
  {
    ExtensionsConfig extensionsConfig = new TestExtensionsConfig(temporaryFolder.getRoot().getPath());
    final ExtensionsLoader extnLoader = new ExtensionsLoader(extensionsConfig, objectMapper);

    final File extensionDir = temporaryFolder.newFolder();
    final File extensionJar = new File(extensionDir, "a.jar");
    createNewJar(extensionJar, jarFileContents);

    final File dependentExtensionDir = temporaryFolder.newFolder();
    final File dependentExtensionJar = new File(dependentExtensionDir, "a.jar");
    final DruidExtensionDependencies druidExtensionDependencies = new DruidExtensionDependencies(ImmutableList.of(extensionDir.getName()));
    createNewJar(dependentExtensionJar, ImmutableMap.of("druid-extension-dependencies.json", objectMapper.writeValueAsBytes(druidExtensionDependencies)));

    StandardURLClassLoader classLoader = extnLoader.getClassLoaderForExtension(extensionDir, false);
    StandardURLClassLoader dependendentClassLoader = extnLoader.getClassLoaderForExtension(dependentExtensionDir, false);
    Assert.assertTrue(dependendentClassLoader.getExtensionDependencyClassLoaders().contains(classLoader));
    Assert.assertEquals(0, classLoader.getExtensionDependencyClassLoaders().size());

  }

  @Test
  public void testGetClassLoaderForExtension_circularDependency() throws IOException
  {
    ExtensionsConfig extensionsConfig = new TestExtensionsConfig(temporaryFolder.getRoot().getPath());
    final ExtensionsLoader extnLoader = new ExtensionsLoader(extensionsConfig, objectMapper);

    final File extensionDir = temporaryFolder.newFolder();
    final File dependentExtensionDir = temporaryFolder.newFolder();

    final File extensionJar = new File(extensionDir, "a.jar");
    final DruidExtensionDependencies druidExtensionDependencies = new DruidExtensionDependencies(ImmutableList.of(dependentExtensionDir.getName()));
    createNewJar(extensionJar, ImmutableMap.of("druid-extension-dependencies.json", objectMapper.writeValueAsBytes(druidExtensionDependencies)));

    final File dependentExtensionJar = new File(dependentExtensionDir, "a.jar");
    final DruidExtensionDependencies druidExtensionDependenciesCircular = new DruidExtensionDependencies(ImmutableList.of(extensionDir.getName()));
    createNewJar(dependentExtensionJar, ImmutableMap.of("druid-extension-dependencies.json", objectMapper.writeValueAsBytes(druidExtensionDependenciesCircular)));

    RE exception = Assert.assertThrows(RE.class, () -> {
      extnLoader.getClassLoaderForExtension(extensionDir, false);
    });

    Assert.assertTrue(exception.getMessage().contains("has a circular druid extension dependency."));
  }

  @Test
  public void testGetClassLoaderForExtension_multipleDruidJars() throws IOException
  {
    ExtensionsConfig extensionsConfig = new TestExtensionsConfig(temporaryFolder.getRoot().getPath());
    final ExtensionsLoader extnLoader = new ExtensionsLoader(extensionsConfig, objectMapper);

    final File extensionDir = temporaryFolder.newFolder();

    final File extensionJar = new File(extensionDir, "a.jar");
    final DruidExtensionDependencies druidExtensionDependencies = new DruidExtensionDependencies(ImmutableList.of());
    createNewJar(extensionJar, ImmutableMap.of("druid-extension-dependencies.json", objectMapper.writeValueAsBytes(druidExtensionDependencies)));

    final File extensionJar2 = new File(extensionDir, "b.jar");
    createNewJar(extensionJar2, ImmutableMap.of("druid-extension-dependencies.json", objectMapper.writeValueAsBytes(druidExtensionDependencies)));


    RE exception = Assert.assertThrows(RE.class, () -> {
      extnLoader.getClassLoaderForExtension(extensionDir, false);
    });

    Assert.assertTrue(
        exception.getMessage().contains("Each jar should be in a separate extension directory.")
    );
  }

  @Test
  public void test_getModules_inSimulationMode_returnsOnlyAllowedModules()
  {
    // Verify with 1 module configured
    final ExtensionsConfig configWithAllowedModules = new ExtensionsConfig() {
      @Override
      public LinkedHashSet<String> getModulesForSimulation()
      {
        return new LinkedHashSet<>(List.of(TestDruidModule.class.getName()));
      }
    };

    final ExtensionsLoader loaderWithAllowedModules = new ExtensionsLoader(configWithAllowedModules, objectMapper);
    final List<DruidModule> modules = List.copyOf(loaderWithAllowedModules.getModules());
    Assert.assertEquals(1, modules.size());
    Assert.assertEquals(TestDruidModule.class, modules.get(0).getClass());

    // Verify with no modules configured
    final ExtensionsConfig configWithNoModules = new ExtensionsConfig() {
      @Override
      public LinkedHashSet<String> getModulesForSimulation()
      {
        return new LinkedHashSet<>();
      }
    };

    final ExtensionsLoader loaderWithNoModules = new ExtensionsLoader(configWithNoModules, objectMapper);
    Assert.assertTrue(loaderWithNoModules.getModules().isEmpty());
  }

  private void createNewJar(File jarFileLocation, Map<String, byte[]> jarFileContents) throws IOException
  {
    Assert.assertTrue(jarFileLocation.createNewFile());
    FileOutputStream fos = new FileOutputStream(jarFileLocation.getPath());
    JarOutputStream jarOut = new JarOutputStream(fos);
    for (Map.Entry<String, byte[]> fileNameToContents : jarFileContents.entrySet()) {
      JarEntry entry = new JarEntry(fileNameToContents.getKey());
      jarOut.putNextEntry(entry);
      jarOut.write(fileNameToContents.getValue());
      jarOut.closeEntry();
    }
    jarOut.close();
    fos.close();
  }

  private static class TestExtensionsConfig extends ExtensionsConfig
  {
    final String directory;

    public TestExtensionsConfig(String directory)
    {
      this.directory = directory;
    }

    @Override
    public String getDirectory()
    {
      return directory;
    }
  }
}
