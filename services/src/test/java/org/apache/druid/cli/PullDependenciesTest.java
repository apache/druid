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

package org.apache.druid.cli;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.tesla.aether.internal.DefaultTeslaAether;
import org.apache.druid.guice.ExtensionsConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.graph.DefaultDependencyNode;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.DependencyFilter;
import org.eclipse.aether.graph.DependencyNode;
import org.eclipse.aether.resolution.DependencyRequest;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PullDependenciesTest
{
  private static final String EXTENSION_A_COORDINATE = "groupX:extension_A:123";
  private static final String EXTENSION_B_COORDINATE = "groupY:extension_B:456";
  private static final String HADOOP_CLIENT_2_3_0_COORDINATE = "org.apache.hadoop:hadoop-client:2.3.0";
  private static final String HADOOP_CLIENT_2_4_0_COORDINATE = "org.apache.hadoop:hadoop-client:2.4.0";

  private static final String DEPENDENCY_GROUPID = "groupid";
  private static final String HADOOP_CLIENT_VULNERABLE_ARTIFACTID1 = "vulnerable1";
  private static final String HADOOP_CLIENT_VULNERABLE_ARTIFACTID2 = "vulnerable2";
  private static final Set<String> HADOOP_CLIENT_VULNERABLE_ARTIFACTIDS = ImmutableSet.of(
      HADOOP_CLIENT_VULNERABLE_ARTIFACTID1,
      HADOOP_CLIENT_VULNERABLE_ARTIFACTID2
  );
  private static final String HADOOP_CLIENT_VULNERABLE_JAR1 = HADOOP_CLIENT_VULNERABLE_ARTIFACTID1 + ".jar";
  private static final String HADOOP_CLIENT_VULNERABLE_JAR2 = HADOOP_CLIENT_VULNERABLE_ARTIFACTID2 + ".jar";
  private static final PullDependencies.Dependencies HADOOP_EXCLUSIONS =
      PullDependencies.Dependencies.builder()
                                   .put(DEPENDENCY_GROUPID, HADOOP_CLIENT_VULNERABLE_ARTIFACTID1)
                                   .put(DEPENDENCY_GROUPID, HADOOP_CLIENT_VULNERABLE_ARTIFACTID2)
                                   .build();

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File localRepo; // a mock local repository that stores jars

  private final Artifact extension_A = new DefaultArtifact(EXTENSION_A_COORDINATE);
  private final Artifact extension_B = new DefaultArtifact(EXTENSION_B_COORDINATE);
  private final Artifact hadoop_client_2_3_0 = new DefaultArtifact(HADOOP_CLIENT_2_3_0_COORDINATE);
  private final Artifact hadoop_client_2_4_0 = new DefaultArtifact(HADOOP_CLIENT_2_4_0_COORDINATE);

  private PullDependencies pullDependencies;
  private File rootExtensionsDir;
  private File rootHadoopDependenciesDir;

  private Map<Artifact, List<String>> extensionToDependency;

  @Before
  public void setUp() throws Exception
  {
    localRepo = temporaryFolder.newFolder("local_repo");
    extensionToDependency = new HashMap<>();

    extensionToDependency.put(extension_A, ImmutableList.of("a", "b", "c"));
    extensionToDependency.put(extension_B, ImmutableList.of("d", "e"));
    extensionToDependency.put(hadoop_client_2_3_0, ImmutableList.of("f", "g"));
    extensionToDependency.put(
        hadoop_client_2_4_0,
        ImmutableList.of("h", "i", HADOOP_CLIENT_VULNERABLE_ARTIFACTID1, HADOOP_CLIENT_VULNERABLE_ARTIFACTID2)
    );

    rootExtensionsDir = temporaryFolder.newFolder("extensions");
    rootHadoopDependenciesDir = temporaryFolder.newFolder("druid_hadoop_dependencies");

    pullDependencies = new PullDependencies(
        new DefaultTeslaAether()
        {
          @Override
          public List<Artifact> resolveArtifacts(DependencyRequest request)
          {
            return getArtifactsForExtension(
                request.getCollectRequest().getRoot().getArtifact(),
                request.getFilter()
            );
          }
        },
        new ExtensionsConfig()
        {
          @Override
          public String getDirectory()
          {
            return rootExtensionsDir.getAbsolutePath();
          }

          @Override
          public String getHadoopDependenciesDir()
          {
            return rootHadoopDependenciesDir.getAbsolutePath();
          }
        },
        HADOOP_EXCLUSIONS
    );

    pullDependencies.coordinates = ImmutableList.of(EXTENSION_A_COORDINATE, EXTENSION_B_COORDINATE);
    pullDependencies.hadoopCoordinates = ImmutableList.of(
        HADOOP_CLIENT_2_3_0_COORDINATE,
        HADOOP_CLIENT_2_4_0_COORDINATE
    );

    // Because --clean is specified, pull-deps will first remove existing root extensions and hadoop dependencies
    pullDependencies.clean = true;
  }

  private List<Artifact> getArtifactsForExtension(Artifact artifact, DependencyFilter filter)
  {
    final List<String> names = extensionToDependency.get(artifact);
    final List<Artifact> artifacts = new ArrayList<>();
    for (String name : names) {
      final File jarFile = new File(localRepo, name + ".jar");
      try {
        jarFile.createNewFile();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }

      DependencyNode node = new DefaultDependencyNode(
          new Dependency(
              new DefaultArtifact(DEPENDENCY_GROUPID, name, null, "jar", "1.0", null, jarFile),
              "compile"
          )
      );
      if (filter.accept(node, Collections.emptyList())) {
        artifacts.add(node.getArtifact());
      }
    }
    return artifacts;
  }

  private List<File> getExpectedJarFiles(Artifact artifact)
  {
    final String artifactId = artifact.getArtifactId();
    final List<String> names = extensionToDependency.get(artifact);
    final List<File> expectedJars;
    if ("hadoop-client".equals(artifactId)) {
      final String version = artifact.getVersion();
      expectedJars = names.stream()
                          .filter(name -> !HADOOP_CLIENT_VULNERABLE_ARTIFACTIDS.contains(name))
                          .map(name -> new File(
                              StringUtils.format(
                                  "%s/%s/%s/%s",
                                  rootHadoopDependenciesDir,
                                  artifactId,
                                  version,
                                  name + ".jar"
                              )
                          ))
                          .collect(Collectors.toList());
    } else {
      expectedJars = names.stream()
                          .map(name -> new File(
                              StringUtils.format(
                                  "%s/%s/%s",
                                  rootExtensionsDir,
                                  artifactId,
                                  name + ".jar"
                              )))
                          .collect(Collectors.toList());
    }
    return expectedJars;
  }

  /**
   * If --clean is not specified and root extension directory already exists, skip creating.
   */
  @Test()
  public void testPullDependencies_root_extension_dir_exists()
  {
    pullDependencies.run();
  }

  /**
   * A file exists on the root extension directory path, but it's not a directory, throw exception.
   */
  @Test(expected = RuntimeException.class)
  public void testPullDependencies_root_extension_dir_bad_state() throws IOException
  {
    Assert.assertTrue(rootExtensionsDir.delete());
    Assert.assertTrue(rootExtensionsDir.createNewFile());
    pullDependencies.run();
  }

  /**
   * If --clean is not specified and hadoop dependencies directory already exists, skip creating.
   */
  @Test()
  public void testPullDependencies_root_hadoop_dependencies_dir_exists()
  {
    pullDependencies.run();
  }

  /**
   * A file exists on the root hadoop dependencies directory path, but it's not a directory, throw exception.
   */
  @Test(expected = RuntimeException.class)
  public void testPullDependencies_root_hadoop_dependencies_dir_bad_state() throws IOException
  {
    Assert.assertTrue(rootHadoopDependenciesDir.delete());
    Assert.assertTrue(rootHadoopDependenciesDir.createNewFile());
    pullDependencies.run();
  }

  @Test
  public void testPullDependencies()
  {
    pullDependencies.run();
    final File[] actualExtensions = rootExtensionsDir.listFiles();
    Arrays.sort(actualExtensions);
    Assert.assertEquals(2, actualExtensions.length);
    Assert.assertEquals(extension_A.getArtifactId(), actualExtensions[0].getName());
    Assert.assertEquals(extension_B.getArtifactId(), actualExtensions[1].getName());

    final List<File> jarsUnderExtensionA = Arrays.asList(actualExtensions[0].listFiles());
    Collections.sort(jarsUnderExtensionA);
    Assert.assertEquals(getExpectedJarFiles(extension_A), jarsUnderExtensionA);

    final List<File> jarsUnderExtensionB = Arrays.asList(actualExtensions[1].listFiles());
    Collections.sort(jarsUnderExtensionB);
    Assert.assertEquals(getExpectedJarFiles(extension_B), jarsUnderExtensionB);

    final File[] actualHadoopDependencies = rootHadoopDependenciesDir.listFiles();
    Arrays.sort(actualHadoopDependencies);
    Assert.assertEquals(1, actualHadoopDependencies.length);
    Assert.assertEquals(hadoop_client_2_3_0.getArtifactId(), actualHadoopDependencies[0].getName());

    final File[] versionDirsUnderHadoopClient = actualHadoopDependencies[0].listFiles();
    Assert.assertEquals(2, versionDirsUnderHadoopClient.length);
    Arrays.sort(versionDirsUnderHadoopClient);
    Assert.assertEquals(hadoop_client_2_3_0.getVersion(), versionDirsUnderHadoopClient[0].getName());
    Assert.assertEquals(hadoop_client_2_4_0.getVersion(), versionDirsUnderHadoopClient[1].getName());

    final List<File> jarsUnder2_3_0 = Arrays.asList(versionDirsUnderHadoopClient[0].listFiles());
    Collections.sort(jarsUnder2_3_0);
    Assert.assertEquals(getExpectedJarFiles(hadoop_client_2_3_0), jarsUnder2_3_0);

    final List<File> jarsUnder2_4_0 = Arrays.asList(versionDirsUnderHadoopClient[1].listFiles());
    Collections.sort(jarsUnder2_4_0);
    Assert.assertEquals(getExpectedJarFiles(hadoop_client_2_4_0), jarsUnder2_4_0);
  }

  @Test
  public void testPullDependeciesExcludesHadoopSecurityVulnerabilities()
  {
    pullDependencies.run();

    File hadoopClient240 = new File(
        rootHadoopDependenciesDir,
        Paths.get(hadoop_client_2_4_0.getArtifactId(), hadoop_client_2_4_0.getVersion())
             .toString()
    );
    Assert.assertTrue(hadoopClient240.exists());

    List<String> dependencies = Arrays.stream(hadoopClient240.listFiles())
                                      .map(File::getName)
                                      .collect(Collectors.toList());
    Assert.assertThat(dependencies, CoreMatchers.not(CoreMatchers.hasItem(HADOOP_CLIENT_VULNERABLE_JAR1)));
    Assert.assertThat(dependencies, CoreMatchers.not(CoreMatchers.hasItem(HADOOP_CLIENT_VULNERABLE_JAR2)));
  }
}
