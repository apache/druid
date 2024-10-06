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
import org.apache.druid.guice.ExtensionsConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.graph.DefaultDependencyNode;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.DependencyNode;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.repository.Authentication;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.Proxy;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactRequest;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResult;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;
import org.eclipse.aether.util.artifact.JavaScopes;
import org.eclipse.aether.util.repository.AuthenticationBuilder;
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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

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
  private static File localRepo; // a mock local repository that stores jars
  private static Map<Artifact, List<String>> extensionToDependency;
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  private final Artifact extension_A = new DefaultArtifact(EXTENSION_A_COORDINATE);
  private final Artifact extension_B = new DefaultArtifact(EXTENSION_B_COORDINATE);
  private final Artifact hadoop_client_2_3_0 = new DefaultArtifact(HADOOP_CLIENT_2_3_0_COORDINATE);
  private final Artifact hadoop_client_2_4_0 = new DefaultArtifact(HADOOP_CLIENT_2_4_0_COORDINATE);
  private PullDependencies pullDependencies;
  private File rootExtensionsDir;
  private File rootHadoopDependenciesDir;

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

    RepositorySystem realRepositorySystem = RealRepositorySystemUtil.newRepositorySystem();
    RepositorySystem spyMockRepositorySystem = spy(realRepositorySystem);
    RepositorySystemSession repositorySystemSession = RealRepositorySystemUtil.newRepositorySystemSession(
        spyMockRepositorySystem,
        localRepo.getPath()
    );

    doAnswer(invocation -> {
      DependencyRequest request = invocation.getArgument(1);
      return mockDependencyResult(request.getCollectRequest().getRoot().getArtifact());
    }).when(spyMockRepositorySystem).resolveDependencies(eq(repositorySystemSession), any(DependencyRequest.class));


    pullDependencies = new PullDependencies(
        spyMockRepositorySystem,
        repositorySystemSession,
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

    pullDependencies.clean = true;
  }

  private DependencyResult mockDependencyResult(Artifact artifact)
  {
    final List<String> names = extensionToDependency.getOrDefault(artifact, Collections.emptyList());
    final List<ArtifactResult> artifacts = new ArrayList<>();
    List<DependencyNode> children = new ArrayList<>();

    for (String name : names) {
      final File jarFile = new File(localRepo, name + ".jar");
      try {
        jarFile.createNewFile();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      Artifact depArtifact = new DefaultArtifact("groupid", name, null, "jar", "1.0",
                                                 null, jarFile
      );
      DependencyNode depNode = new DefaultDependencyNode(new Dependency(depArtifact, JavaScopes.COMPILE));
      children.add(depNode);
      ArtifactResult artifactResult = new ArtifactResult(new ArtifactRequest(depNode));
      artifactResult.setArtifact(depArtifact);
      artifacts.add(artifactResult);
    }

    DependencyNode rootNode = new DefaultDependencyNode(new Dependency(artifact, JavaScopes.COMPILE));
    rootNode.setChildren(children);

    DependencyResult result = new DependencyResult(new DependencyRequest());
    result.setRoot(rootNode);
    result.setArtifactResults(artifacts);
    return result;
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

  @Test
  public void testPullDependenciesCleanFlag() throws IOException
  {
    File dummyFile1 = new File(rootExtensionsDir, "dummy.txt");
    File dummyFile2 = new File(rootHadoopDependenciesDir, "dummy.txt");
    Assert.assertTrue(dummyFile1.createNewFile());
    Assert.assertTrue(dummyFile2.createNewFile());

    pullDependencies.clean = true;
    pullDependencies.run();

    Assert.assertFalse(dummyFile1.exists());
    Assert.assertFalse(dummyFile2.exists());
  }

  @Test
  public void testPullDependenciesNoDefaultRemoteRepositories()
  {
    pullDependencies.noDefaultRemoteRepositories = true;
    pullDependencies.remoteRepositories = ImmutableList.of("https://custom.repo");

    pullDependencies.run();

    List<RemoteRepository> repositories = pullDependencies.getRemoteRepositories();
    Assert.assertEquals(1, repositories.size());
    Assert.assertEquals("https://custom.repo", repositories.get(0).getUrl());
  }

  @Test
  public void testPullDependenciesDirectoryCreationFailure() throws IOException
  {
    if (rootExtensionsDir.exists()) {
      rootExtensionsDir.delete();
    }
    Assert.assertTrue(rootExtensionsDir.createNewFile());

    Assert.assertThrows(IllegalArgumentException.class, () -> pullDependencies.run());
  }

  @Test
  public void testGetArtifactWithValidCoordinate()
  {
    String coordinate = "groupX:artifactX:1.0.0";
    DefaultArtifact artifact = (DefaultArtifact) pullDependencies.getArtifact(coordinate);
    Assert.assertEquals("groupX", artifact.getGroupId());
    Assert.assertEquals("artifactX", artifact.getArtifactId());
    Assert.assertEquals("1.0.0", artifact.getVersion());
  }

  @Test
  public void testGetArtifactwithCoordinateWithoutDefaultVersion()
  {
    String coordinate = "groupY:artifactY";
    Assert.assertThrows(
        "Bad artifact coordinates groupY:artifactY, expected format is <groupId>:<artifactId>[:<extension>[:<classifier>]]:<version>",
        IllegalArgumentException.class,
        () -> pullDependencies.getArtifact(coordinate)
    );

  }

  @Test
  public void testGetArtifactWithCoordinateWithoutVersion()
  {
    pullDependencies.defaultVersion = "2.0.0";
    String coordinate = "groupY:artifactY";
    DefaultArtifact artifact = (DefaultArtifact) pullDependencies.getArtifact(coordinate);
    Assert.assertEquals("groupY", artifact.getGroupId());
    Assert.assertEquals("artifactY", artifact.getArtifactId());
    Assert.assertEquals("2.0.0", artifact.getVersion());
  }

  @Test
  public void testGetRemoteRepositoriesWithDefaultRepositories()
  {
    pullDependencies.noDefaultRemoteRepositories = false; // Use default remote repositories
    pullDependencies.remoteRepositories = ImmutableList.of("https://custom.repo");

    List<RemoteRepository> repositories = pullDependencies.getRemoteRepositories();
    Assert.assertEquals(2, repositories.size());
    Assert.assertEquals("https://repo1.maven.org/maven2/", repositories.get(0).getUrl());
    Assert.assertEquals("https://custom.repo", repositories.get(1).getUrl());
  }

  @Test
  public void testGetRepositorySystemSessionWithProxyConfiguration()
  {
    pullDependencies.useProxy = true;
    pullDependencies.proxyType = "http";
    pullDependencies.proxyHost = "localhost";
    pullDependencies.proxyPort = 8080;
    pullDependencies.proxyUsername = "user";
    pullDependencies.proxyPassword = "password";

    DefaultRepositorySystemSession session = (DefaultRepositorySystemSession) pullDependencies.getRepositorySystemSession();

    LocalRepository localRepo = session.getLocalRepositoryManager().getRepository();
    Assert.assertEquals(pullDependencies.localRepository, localRepo.getBasedir().getAbsolutePath());

    Proxy proxy = session.getProxySelector().getProxy(
        new RemoteRepository.Builder("test", "default", "http://example.com").build()
    );
    RemoteRepository testRepository = new RemoteRepository.Builder("test", "default", "http://example.com")
        .setProxy(proxy)
        .build();

    Assert.assertNotNull(proxy);
    Assert.assertEquals("localhost", proxy.getHost());
    Assert.assertEquals(8080, proxy.getPort());
    Assert.assertEquals("http", proxy.getType());

    Authentication auth = new AuthenticationBuilder().addUsername("user").addPassword("password").build();
    Assert.assertEquals(auth, proxy.getAuthentication());
  }

  @Test
  public void testGetRepositorySystemSessionWithoutProxyConfiguration()
  {
    pullDependencies.useProxy = false;
    DefaultRepositorySystemSession session = (DefaultRepositorySystemSession) pullDependencies.getRepositorySystemSession();
    LocalRepository localRepo = session.getLocalRepositoryManager().getRepository();
    Assert.assertEquals(pullDependencies.localRepository, localRepo.getBasedir().getAbsolutePath());
    Proxy proxy = session.getProxySelector().getProxy(
        new RemoteRepository.Builder("test", "default", "http://example.com").build()
    );
    Assert.assertNull(proxy);
  }

  private static class RealRepositorySystemUtil
  {
    public static RepositorySystem newRepositorySystem()
    {
      DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
      locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
      locator.addService(TransporterFactory.class, HttpTransporterFactory.class);
      return locator.getService(RepositorySystem.class);
    }

    public static DefaultRepositorySystemSession newRepositorySystemSession(
        RepositorySystem system,
        String localRepoPath
    )
    {
      DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();

      LocalRepository localRepo = new LocalRepository(localRepoPath);
      session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));

      return session;
    }
  }

}
