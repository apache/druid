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
import org.apache.druid.guice.ExtensionsConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.junit5.ExpectedToThrow;
import org.apache.druid.testing.junit5.JUnit5Assertions;
import org.apache.druid.testing.junit5.TempDirExtension;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class PullDependenciesTest
{
  private static final String EXTENSION_A_COORDINATE = "groupX:extension_A:123";
  private static final String EXTENSION_B_COORDINATE = "groupY:extension_B:456";

  private static final String DEPENDENCY_GROUPID = "groupid";
  private static File localRepo; // a mock local repository that stores jars
  private static Map<Artifact, List<String>> extensionToDependency;
  @RegisterExtension
  public final TempDirExtension temporaryFolder = new TempDirExtension();
  private final Artifact extension_A = new DefaultArtifact(EXTENSION_A_COORDINATE);
  private final Artifact extension_B = new DefaultArtifact(EXTENSION_B_COORDINATE);
  private PullDependencies pullDependencies;
  private File rootExtensionsDir;

  @BeforeEach
  public void setUp() throws Exception
  {
    localRepo = temporaryFolder.newFolder("local_repo");
    extensionToDependency = new HashMap<>();

    extensionToDependency.put(extension_A, ImmutableList.of("a", "b", "c"));
    extensionToDependency.put(extension_B, ImmutableList.of("d", "e"));

    rootExtensionsDir = temporaryFolder.newFolder("extensions");

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
        }
    );

    pullDependencies.coordinates = ImmutableList.of(EXTENSION_A_COORDINATE, EXTENSION_B_COORDINATE);

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
    return names.stream()
                .map(name -> new File(
                            StringUtils.format(
                                "%s/%s/%s",
                                rootExtensionsDir,
                                artifactId,
                                name + ".jar"
                            )))
                .collect(Collectors.toList());
  }

  /**
   * If --clean is not specified and root extension directory already exists, skip creating.
   */
  @Test
  public void testPullDependencies_root_extension_dir_exists()
  {
    pullDependencies.run();
  }

  /**
   * A file exists on the root extension directory path, but it's not a directory, throw exception.
   */
  @Test
  @ExpectedToThrow(RuntimeException.class)
  public void testPullDependencies_root_extension_dir_bad_state() throws IOException
  {
    JUnit5Assertions.assertTrue(rootExtensionsDir.delete());
    JUnit5Assertions.assertTrue(rootExtensionsDir.createNewFile());
    pullDependencies.run();
  }

  @Test
  public void testPullDependencies()
  {
    pullDependencies.run();
    final File[] actualExtensions = rootExtensionsDir.listFiles();
    Arrays.sort(actualExtensions);
    JUnit5Assertions.assertEquals(2, actualExtensions.length);
    JUnit5Assertions.assertEquals(extension_A.getArtifactId(), actualExtensions[0].getName());
    JUnit5Assertions.assertEquals(extension_B.getArtifactId(), actualExtensions[1].getName());

    final List<File> jarsUnderExtensionA = Arrays.asList(actualExtensions[0].listFiles());
    Collections.sort(jarsUnderExtensionA);
    JUnit5Assertions.assertEquals(getExpectedJarFiles(extension_A), jarsUnderExtensionA);

    final List<File> jarsUnderExtensionB = Arrays.asList(actualExtensions[1].listFiles());
    Collections.sort(jarsUnderExtensionB);
    JUnit5Assertions.assertEquals(getExpectedJarFiles(extension_B), jarsUnderExtensionB);
  }

  @Test
  public void testPullDependenciesCleanFlag() throws IOException
  {
    File dummyFile1 = new File(rootExtensionsDir, "dummy.txt");
    JUnit5Assertions.assertTrue(dummyFile1.createNewFile());

    pullDependencies.clean = true;
    pullDependencies.run();

    JUnit5Assertions.assertFalse(dummyFile1.exists());
  }

  @Test
  public void testPullDependenciesNoDefaultRemoteRepositories()
  {
    pullDependencies.noDefaultRemoteRepositories = true;
    pullDependencies.remoteRepositories = ImmutableList.of("https://custom.repo");

    pullDependencies.run();

    List<RemoteRepository> repositories = pullDependencies.getRemoteRepositories();
    JUnit5Assertions.assertEquals(1, repositories.size());
    JUnit5Assertions.assertEquals(repositories.get(0).getUrl(), "https://custom.repo");
  }

  @Test
  public void testPullDependenciesDirectoryCreationFailure() throws IOException
  {
    if (rootExtensionsDir.exists()) {
      rootExtensionsDir.delete();
    }
    JUnit5Assertions.assertTrue(rootExtensionsDir.createNewFile());

    JUnit5Assertions.assertThrows(IllegalArgumentException.class, () -> pullDependencies.run());
  }

  @Test
  public void testGetArtifactWithValidCoordinate()
  {
    String coordinate = "groupX:artifactX:1.0.0";
    DefaultArtifact artifact = (DefaultArtifact) pullDependencies.getArtifact(coordinate);
    JUnit5Assertions.assertEquals(artifact.getGroupId(), "groupX");
    JUnit5Assertions.assertEquals(artifact.getArtifactId(), "artifactX");
    JUnit5Assertions.assertEquals(artifact.getVersion(), "1.0.0");
  }

  @Test
  public void testGetArtifactwithCoordinateWithoutDefaultVersion()
  {
    String coordinate = "groupY:artifactY";
    JUnit5Assertions.assertThrows(IllegalArgumentException.class, () -> pullDependencies.getArtifact(coordinate), "Bad artifact coordinates groupY:artifactY, expected format is <groupId>:<artifactId>[:<extension>[:<classifier>]]:<version>");

  }

  @Test
  public void testGetArtifactWithCoordinateWithoutVersion()
  {
    pullDependencies.defaultVersion = "2.0.0";
    String coordinate = "groupY:artifactY";
    DefaultArtifact artifact = (DefaultArtifact) pullDependencies.getArtifact(coordinate);
    JUnit5Assertions.assertEquals(artifact.getGroupId(), "groupY");
    JUnit5Assertions.assertEquals(artifact.getArtifactId(), "artifactY");
    JUnit5Assertions.assertEquals(artifact.getVersion(), "2.0.0");
  }

  @Test
  public void testGetRemoteRepositoriesWithDefaultRepositories()
  {
    pullDependencies.noDefaultRemoteRepositories = false; // Use default remote repositories
    pullDependencies.remoteRepositories = ImmutableList.of("https://custom.repo");

    List<RemoteRepository> repositories = pullDependencies.getRemoteRepositories();
    JUnit5Assertions.assertEquals(2, repositories.size());
    JUnit5Assertions.assertEquals(repositories.get(0).getUrl(), "https://repo1.maven.org/maven2/");
    JUnit5Assertions.assertEquals(repositories.get(1).getUrl(), "https://custom.repo");
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
    JUnit5Assertions.assertEquals(pullDependencies.localRepository, localRepo.getBasedir().getAbsolutePath());

    Proxy proxy = session.getProxySelector().getProxy(
        new RemoteRepository.Builder("test", "default", "http://example.com").build()
    );
    RemoteRepository testRepository = new RemoteRepository.Builder("test", "default", "http://example.com")
        .setProxy(proxy)
        .build();

    JUnit5Assertions.assertNotNull(proxy);
    JUnit5Assertions.assertEquals(proxy.getHost(), "localhost");
    JUnit5Assertions.assertEquals(8080, proxy.getPort());
    JUnit5Assertions.assertEquals(proxy.getType(), "http");

    Authentication auth = new AuthenticationBuilder().addUsername("user").addPassword("password").build();
    JUnit5Assertions.assertEquals(auth, proxy.getAuthentication());
  }

  @Test
  public void testGetRepositorySystemSessionWithoutProxyConfiguration()
  {
    pullDependencies.useProxy = false;
    DefaultRepositorySystemSession session = (DefaultRepositorySystemSession) pullDependencies.getRepositorySystemSession();
    LocalRepository localRepo = session.getLocalRepositoryManager().getRepository();
    JUnit5Assertions.assertEquals(pullDependencies.localRepository, localRepo.getBasedir().getAbsolutePath());
    Proxy proxy = session.getProxySelector().getProxy(
        new RemoteRepository.Builder("test", "default", "http://example.com").build()
    );
    JUnit5Assertions.assertNull(proxy);
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
