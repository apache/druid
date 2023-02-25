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
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class ExtensionsLoaderTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private Injector startupInjector()
  {
    // There is no extensions directory for this test. Must explicitly say so.
    Properties properties = new Properties();
    properties.put(ExtensionsConfig.PROPERTY_BASE + ".directory", "");
    return new StartupInjectorBuilder()
        .withProperties(properties)
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

    String key = extensionDir.getName();
    extnLoader.getLoadersMap()
                  .put(key, new URLClassLoader(new URL[]{}, ExtensionsLoader.class.getClassLoader()));

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
    final ExtensionsLoader extnLoader = new ExtensionsLoader(new ExtensionsConfig());

    final File some_extension_dir = temporaryFolder.newFolder();
    final File a_jar = new File(some_extension_dir, "a.jar");
    final File b_jar = new File(some_extension_dir, "b.jar");
    final File c_jar = new File(some_extension_dir, "c.jar");
    a_jar.createNewFile();
    b_jar.createNewFile();
    c_jar.createNewFile();
    final URLClassLoader loader = extnLoader.getClassLoaderForExtension(some_extension_dir);
    final URL[] expectedURLs = new URL[]{a_jar.toURI().toURL(), b_jar.toURI().toURL(), c_jar.toURI().toURL()};
    final URL[] actualURLs = loader.getURLs();
    Arrays.sort(actualURLs, Comparator.comparing(URL::getPath));
    Assert.assertArrayEquals(expectedURLs, actualURLs);
    Assert.assertSame(loader, extnLoader.getClassLoaderForExtension(some_extension_dir.getName()));
  }

  @Test
  public void testGetLoadedModules()
  {
    final ExtensionsConfig config = ExtensionsConfig.builder()
        .directory("")
        .build();
    final ExtensionsLoader extnLoader = new ExtensionsLoader(config);
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
  public void testGetExtensionFilesToLoad_no_extensions_dir() throws IOException
  {
    final ExtensionsConfig config = ExtensionsConfig.builder()
        .directory("")
        .build();
    final ExtensionsLoader extnLoader = new ExtensionsLoader(config);
    Assert.assertTrue(
        "Empty root extensionsDir should return an empty list",
        extnLoader.getExtensionFilesToLoad().isEmpty()
    );
  }

  /**
   * For backward compatibility, allow the default "extensions" directory to be
   * missing as long as the load list is empty.
   */
  @Test
  public void testGetExtensionFilesToLoad_default_extensions_dir() throws IOException
  {
    final ExtensionsConfig config = new ExtensionsConfig();
    final ExtensionsLoader extnLoader = new ExtensionsLoader(config);
    Assert.assertTrue(
        "Empty root extensionsDir should return an empty list",
        extnLoader.getExtensionFilesToLoad().isEmpty()
    );
  }

  /**
   * If the default "extensions" directory is missing, but there is a load list,
   * then complain about the missing directory.
   */
  @Test
  public void testGetExtensionFilesToLoad_default_with_load_list() throws IOException
  {
    final ExtensionsConfig config = ExtensionsConfig.builder()
        .loadList(Collections.singletonList("foo"))
        .build();
    final ExtensionsLoader extnLoader = new ExtensionsLoader(config);
    Assert.assertThrows(ISE.class, () -> extnLoader.getExtensionFilesToLoad());
  }

  @Test
  public void testGetExtensionFilesToLoad_non_exist_extensions_dir() throws IOException
  {
    final File tmpDir = temporaryFolder.newFolder();
    Assert.assertTrue("could not create missing folder", !tmpDir.exists() || tmpDir.delete());
    final ExtensionsConfig config = ExtensionsConfig.builder()
        .directory(tmpDir.getAbsolutePath())
        .build();
    final ExtensionsLoader extnLoader = new ExtensionsLoader(config);
    Assert.assertThrows(ISE.class, () -> extnLoader.getExtensionFilesToLoad());
  }

  @Test
  public void testGetExtensionFilesToLoad_wrong_type_extensions_dir() throws IOException
  {
    final File extensionsDir = temporaryFolder.newFile();
    final ExtensionsConfig config = ExtensionsConfig.builder()
        .directory(extensionsDir.getAbsolutePath())
        .build();
    final ExtensionsLoader extnLoader = new ExtensionsLoader(config);
    Assert.assertThrows(ISE.class, () -> extnLoader.getExtensionFilesToLoad());
  }

  @Test
  public void testGetExtensionFilesToLoad_empty_extensions_dir() throws IOException
  {
    final File extensionsDir = temporaryFolder.newFolder();
    final ExtensionsConfig config = ExtensionsConfig.builder()
        .directory(extensionsDir.getAbsolutePath())
        .build();

    final ExtensionsLoader extnLoader = new ExtensionsLoader(config);
    Assert.assertTrue(
        "Empty root extensionsDir should return an empty list",
        extnLoader.getExtensionFilesToLoad().isEmpty()
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
    final ExtensionsConfig config = ExtensionsConfig.builder()
        .directory(extensionsDir.getAbsolutePath())
        .build();
    final ExtensionsLoader extnLoader = new ExtensionsLoader(config);
    final File mysql_metadata_storage = new File(extensionsDir, "mysql-metadata-storage");
    mysql_metadata_storage.mkdir();

    final List<File> expectedFileList = Arrays.asList(mysql_metadata_storage);
    final List<File> actualFileList = extnLoader.getExtensionFilesToLoad();
    Collections.sort(actualFileList);
    Assert.assertEquals(expectedFileList, actualFileList);
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

    final ExtensionsConfig config = ExtensionsConfig.builder()
        .directory(extensionsDir.getAbsolutePath())
        .loadList(Arrays.asList("mysql-metadata-storage", absolutePathExtension.getAbsolutePath()))
        .build();
    final ExtensionsLoader extnLoader = new ExtensionsLoader(config);
    final File mysql_metadata_storage = new File(extensionsDir, "mysql-metadata-storage");
    final File random_extension = new File(extensionsDir, "random-extensions");

    mysql_metadata_storage.mkdir();
    random_extension.mkdir();

    final List<File> expectedFileList = Arrays.asList(mysql_metadata_storage, absolutePathExtension);
    final List<File> actualFileList = extnLoader.getExtensionFilesToLoad();
    Assert.assertEquals(expectedFileList, actualFileList);
  }

  @Test
  public void testGetExtensionFilesToLoad_with_load_list_and_path() throws IOException
  {
    // The loader only looks for directories: doesn't matter if they are empty.
    final File extensionsDir1 = temporaryFolder.newFolder();
    String extn1 = "extension1";
    File extn1Dir = new File(extensionsDir1, extn1);
    extn1Dir.mkdir();
    final File extensionsDir2 = temporaryFolder.newFolder();
    String extn2 = "extension2";
    File extn2Dir = new File(extensionsDir2, extn2);
    extn2Dir.mkdir();

    final ExtensionsConfig config = ExtensionsConfig.builder()
        .directory(extensionsDir1.getAbsolutePath())
        .path(Collections.singletonList(extensionsDir2.getAbsolutePath()))
        .loadList(Arrays.asList(extn1, extn2))
        .build();
    final ExtensionsLoader extnLoader = new ExtensionsLoader(config);

    final List<File> expectedFileList = Arrays.asList(extn1Dir, extn2Dir);
    final List<File> actualFileList = extnLoader.getExtensionFilesToLoad();
    Assert.assertEquals(expectedFileList, actualFileList);
  }

  @Test
  public void testGetExtensionFilesToLoad_with_load_list_with_path_only() throws IOException
  {
    // The loader only looks for directories: doesn't matter if they are empty.
    final File extensionsDir1 = temporaryFolder.newFolder();
    final File extensionsDir2 = temporaryFolder.newFolder();

    // For fun, mix up the order
    String extn1 = "extension1";
    File extn1Dir = new File(extensionsDir2, extn1);
    extn1Dir.mkdir();
    String extn2 = "extension2";
    File extn2Dir = new File(extensionsDir1, extn2);
    extn2Dir.mkdir();

    final List<File> expectedFileList = Arrays.asList(extn1Dir, extn2Dir);

    {
      // No directory. But, the user can never achieve this since a null directory
      // in the input means use the default.
      final ExtensionsConfig config = ExtensionsConfig.builder()
          .directory(null)
          .path(Arrays.asList(extensionsDir1.getAbsolutePath(), extensionsDir2.getAbsolutePath()))
          .loadList(Arrays.asList(extn1, extn2))
          .build();
      final ExtensionsLoader extnLoader = new ExtensionsLoader(config);

      final List<File> actualFileList = extnLoader.getExtensionFilesToLoad();
      Assert.assertEquals(expectedFileList, actualFileList);
    }

    {
      // Blank directory, which is what the user can provide.
      final ExtensionsConfig config = ExtensionsConfig.builder()
          .directory("")
          .path(Arrays.asList(extensionsDir1.getAbsolutePath(), extensionsDir2.getAbsolutePath()))
          .loadList(Arrays.asList(extn1, extn2))
          .build();
      final ExtensionsLoader extnLoader = new ExtensionsLoader(config);

      final List<File> actualFileList = extnLoader.getExtensionFilesToLoad();
      Assert.assertEquals(expectedFileList, actualFileList);
    }

    {
      // Sanity check: reverse the order. Results should be the same.
      final ExtensionsConfig config = ExtensionsConfig.builder()
          .directory("")
          .path(Arrays.asList(extensionsDir2.getAbsolutePath(), extensionsDir1.getAbsolutePath()))
          .loadList(Arrays.asList(extn1, extn2))
          .build();
      final ExtensionsLoader extnLoader = new ExtensionsLoader(config);

      final List<File> actualFileList = extnLoader.getExtensionFilesToLoad();
      Assert.assertEquals(expectedFileList, actualFileList);
    }
  }

  /**
   * druid.extension.loadList is specified, but contains an extension that is not prepared under root extension directory.
   * Initialization.getExtensionFilesToLoad is supposed to throw ISE.
   */
  @Test
  public void testGetExtensionFilesToLoad_with_non_exist_item_in_load_list() throws IOException
  {
    final File extensionsDir = temporaryFolder.newFolder();
    final ExtensionsConfig config = ExtensionsConfig.builder()
        .directory(extensionsDir.getAbsolutePath())
        .loadList(Collections.singletonList("mysql-metadata-storage"))
        .build();
    final File random_extension = new File(extensionsDir, "random-extensions");
    random_extension.mkdir();
    final ExtensionsLoader extnLoader = new ExtensionsLoader(config);
    Assert.assertThrows(ISE.class, () -> extnLoader.getExtensionFilesToLoad());
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
}
