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

package io.druid.cli;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.druid.guice.ExtensionsConfig;
import io.druid.java.util.common.ISE;
import io.tesla.aether.internal.DefaultTeslaAether;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 */
public class PullDependenciesTest
{

  private static final String EXTENSION_A_COORDINATE = "groupX:extension_A:123";
  private static final String EXTENSION_B_COORDINATE = "groupY:extension_B:456";
  private static final String HADOOP_CLIENT_2_3_0_COORDINATE = "org.apache.hadoop:hadoop-client:2.3.0";
  private static final String HADOOP_CLIENT_2_4_0_COORDINATE = "org.apache.hadoop:hadoop-client:2.4.0";

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

  private HashMap<Artifact, List<String>> extensionToJars; // map Artifact to its associated jars' names

  @Before
  public void setUp() throws Exception
  {
    localRepo = temporaryFolder.newFolder();
    extensionToJars = new HashMap<>();

    extensionToJars.put(extension_A, ImmutableList.of("a.jar", "b.jar", "c.jar"));
    extensionToJars.put(extension_B, ImmutableList.of("d.jar", "e.jar"));
    extensionToJars.put(hadoop_client_2_3_0, ImmutableList.of("f.jar", "g.jar"));
    extensionToJars.put(hadoop_client_2_4_0, ImmutableList.of("h.jar", "i.jar"));

    rootExtensionsDir = new File(temporaryFolder.getRoot(), "extensions");
    rootHadoopDependenciesDir = new File(temporaryFolder.getRoot(), "druid_hadoop_dependencies");

    pullDependencies = new PullDependencies(
        new DefaultTeslaAether()
        {
          @Override
          public List<Artifact> resolveArtifacts(DependencyRequest request) throws DependencyResolutionException
          {
            return getArtifactsForExtension(request.getCollectRequest().getRoot().getArtifact());
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
        }
    );

    pullDependencies.coordinates = ImmutableList.of(EXTENSION_A_COORDINATE, EXTENSION_B_COORDINATE);
    pullDependencies.hadoopCoordinates = ImmutableList.of(
        HADOOP_CLIENT_2_3_0_COORDINATE,
        HADOOP_CLIENT_2_4_0_COORDINATE
    );
  }

  private List<Artifact> getArtifactsForExtension(Artifact artifact)
  {
    final List<String> jarNames = extensionToJars.get(artifact);
    final List<Artifact> artifacts = Lists.newArrayList();
    for (String jarName : jarNames) {
      final File jarFile = new File(localRepo, jarName);
      try {
        jarFile.createNewFile();
      }
      catch (IOException e) {
        Throwables.propagate(e);
      }
      artifacts.add(new DefaultArtifact(null, jarName, null, "jar", "1.0", null, jarFile));
    }
    return artifacts;
  }

  private File[] getExpectedJarFiles(Artifact artifact)
  {
    final String artifactId = artifact.getArtifactId();
    final List<String> jarNames = extensionToJars.get(artifact);
    final File[] expectedJars = new File[jarNames.size()];
    if (artifactId.equals("hadoop-client")) {
      final String version = artifact.getVersion();
      for (int i = 0; i < jarNames.size(); ++i) {
        expectedJars[i] = new File(
            String.format(
                "%s/%s/%s/%s",
                rootHadoopDependenciesDir,
                artifactId,
                version,
                jarNames.get(i)
            )
        );
      }
    } else {
      for (int i = 0; i < jarNames.size(); ++i) {
        expectedJars[i] = new File(String.format("%s/%s/%s", rootExtensionsDir, artifactId, jarNames.get(i)));
      }
    }
    return expectedJars;
  }

  /**
   * If --clean is not specified and root extension directory already exists, skip creating.
   */
  @Test()
  public void testPullDependencies_root_extension_dir_exists()
  {
    rootExtensionsDir.mkdir();
    pullDependencies.run();
  }

  /**
   * A file exists on the root extension directory path, but it's not a directory, throw ISE.
   */
  @Test(expected = ISE.class)
  public void testPullDependencies_root_extension_dir_bad_state() throws IOException
  {
    Assert.assertTrue(rootExtensionsDir.createNewFile());
    pullDependencies.run();
  }

  /**
   * If --clean is not specified and hadoop dependencies directory already exists, skip creating.
   */
  @Test()
  public void testPullDependencies_root_hadoop_dependencies_dir_exists()
  {
    rootHadoopDependenciesDir.mkdir();
    pullDependencies.run();
  }

  /**
   * A file exists on the root hadoop dependencies directory path, but it's not a directory, throw ISE.
   */
  @Test(expected = ISE.class)
  public void testPullDependencies_root_hadoop_dependencies_dir_bad_state() throws IOException
  {
    Assert.assertTrue(rootHadoopDependenciesDir.createNewFile());
    pullDependencies.run();
  }

  @Test
  public void testPullDependencies()
  {
    rootExtensionsDir.mkdir();
    rootHadoopDependenciesDir.mkdir();
    // Because --clean is specified, pull-deps will first remove existing root extensions and hadoop dependencies
    pullDependencies.clean = true;

    pullDependencies.run();
    final File[] actualExtensions = rootExtensionsDir.listFiles();
    Arrays.sort(actualExtensions);
    Assert.assertEquals(2, actualExtensions.length);
    Assert.assertEquals(extension_A.getArtifactId(), actualExtensions[0].getName());
    Assert.assertEquals(extension_B.getArtifactId(), actualExtensions[1].getName());

    final File[] jarsUnderExtensionA = actualExtensions[0].listFiles();
    Arrays.sort(jarsUnderExtensionA);
    Assert.assertArrayEquals(getExpectedJarFiles(extension_A), jarsUnderExtensionA);

    final File[] jarsUnderExtensionB = actualExtensions[1].listFiles();
    Arrays.sort(jarsUnderExtensionB);
    Assert.assertArrayEquals(getExpectedJarFiles(extension_B), jarsUnderExtensionB);

    final File[] actualHadoopDependencies = rootHadoopDependenciesDir.listFiles();
    Arrays.sort(actualHadoopDependencies);
    Assert.assertEquals(1, actualHadoopDependencies.length);
    Assert.assertEquals(hadoop_client_2_3_0.getArtifactId(), actualHadoopDependencies[0].getName());

    final File[] versionDirsUnderHadoopClient = actualHadoopDependencies[0].listFiles();
    Assert.assertEquals(2, versionDirsUnderHadoopClient.length);
    Arrays.sort(versionDirsUnderHadoopClient);
    Assert.assertEquals(hadoop_client_2_3_0.getVersion(), versionDirsUnderHadoopClient[0].getName());
    Assert.assertEquals(hadoop_client_2_4_0.getVersion(), versionDirsUnderHadoopClient[1].getName());

    final File[] jarsUnder2_3_0 = versionDirsUnderHadoopClient[0].listFiles();
    Arrays.sort(jarsUnder2_3_0);
    Assert.assertArrayEquals(getExpectedJarFiles(hadoop_client_2_3_0), jarsUnder2_3_0);

    final File[] jarsUnder2_4_0 = versionDirsUnderHadoopClient[1].listFiles();
    Arrays.sort(jarsUnder2_4_0);
    Assert.assertArrayEquals(getExpectedJarFiles(hadoop_client_2_4_0), jarsUnder2_4_0);
  }
}
