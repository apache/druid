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

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import org.apache.druid.guice.ExtensionsConfig;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.maven.artifact.resolver.ArtifactNotFoundException;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.DependencyNode;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.Proxy;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.eclipse.aether.resolution.DependencyResult;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;
import org.eclipse.aether.util.artifact.JavaScopes;
import org.eclipse.aether.util.filter.DependencyFilterUtils;
import org.eclipse.aether.util.repository.AuthenticationBuilder;
import org.eclipse.aether.util.repository.DefaultProxySelector;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Command(
    name = "pull-deps",
    description = "Pull down dependencies to the local repository specified by druid.extensions.localRepository, extensions directory specified by druid.extensions.extensionsDir and hadoop dependencies directory specified by druid.extensions.hadoopDependenciesDir"
)
public class PullDependencies implements Runnable
{
  private static final Logger log = new Logger(PullDependencies.class);

  private static final List<String> DEFAULT_REMOTE_REPOSITORIES = ImmutableList.of(
      "https://repo1.maven.org/maven2/"
  );

  private static final Dependencies PROVIDED_BY_CORE_DEPENDENCIES =
      Dependencies.builder()
                  .put("com.squareup.okhttp", "okhttp")
                  .put("commons-beanutils", "commons-beanutils")
                  .put("org.apache.commons", "commons-compress")
                  .put("org.apache.zookeeper", "zookeeper")
                  .put("com.fasterxml.jackson.core", "jackson-databind")
                  .put("com.fasterxml.jackson.core", "jackson-core")
                  .put("com.fasterxml.jackson.core", "jackson-annotations")
                  .build();

  private static final Dependencies SECURITY_VULNERABILITY_EXCLUSIONS =
      Dependencies.builder()
                  .put("commons-beanutils", "commons-beanutils-core")
                  .build();

  private final Dependencies hadoopExclusions;

  @Inject
  public ExtensionsConfig extensionsConfig;

  @Option(
      name = {"-c", "--coordinate"},
      title = "coordinate",
      description = "Extension coordinate to pull down, followed by a maven coordinate, e.g. org.apache.druid.extensions:mysql-metadata-storage"
  )
  public List<String> coordinates = new ArrayList<>();

  @Option(
      name = {"-h", "--hadoop-coordinate"},
      title = "hadoop coordinate",
      description = "Hadoop dependency to pull down, followed by a maven coordinate, e.g. org.apache.hadoop:hadoop-client:2.4.0"
  )
  public List<String> hadoopCoordinates = new ArrayList<>();

  @Option(
      name = "--no-default-hadoop",
      description = "Don't pull down the default hadoop coordinate, i.e., org.apache.hadoop:hadoop-client-runtime if hadoop3. If `-h` option is supplied, then default hadoop coordinate will not be downloaded."
  )
  public boolean noDefaultHadoop = false;

  @Option(
      name = "--clean",
      title = "Remove exisiting extension and hadoop dependencies directories before pulling down dependencies."
  )
  public boolean clean = false;

  @Option(
      name = {"-l", "--localRepository"},
      title = "A local repository that Maven will use to put downloaded files. Then pull-deps will lay these files out into the extensions directory as needed."
  )
  public String localRepository = StringUtils.format("%s/%s", System.getProperty("user.home"), ".m2/repository");
  @Option(
      name = "--no-default-remote-repositories",
      description = "Don't use the default remote repositories, only use the repositories provided directly via --remoteRepository"
  )
  public boolean noDefaultRemoteRepositories = false;
  @Option(
      name = {"-d", "--defaultVersion"},
      title = "Version to use for extension artifacts without version information."
  )
  public String defaultVersion = PullDependencies.class.getPackage().getImplementationVersion();
  @Option(
      name = {"--use-proxy"},
      title = "Use http/https proxy to pull dependencies."
  )
  public boolean useProxy = false;
  @Option(
      name = {"--proxy-type"},
      title = "The proxy type, should be either http or https"
  )
  public String proxyType = "https";
  @Option(
      name = {"--proxy-host"},
      title = "The proxy host"
  )
  public String proxyHost = "";
  @Option(
      name = {"--proxy-port"},
      title = "The proxy port"
  )
  public int proxyPort = -1;
  @Option(
      name = {"--proxy-username"},
      title = "The proxy username"
  )
  public String proxyUsername = "";
  @Option(
      name = {"--proxy-password"},
      title = "The proxy password"
  )
  public String proxyPassword = "";
  @Option(
      name = {"-r", "--remoteRepository"},
      title = "Add a remote repository. Unless --no-default-remote-repositories is provided, these will be used after https://repo1.maven.org/maven2/"
  )
  List<String> remoteRepositories = new ArrayList<>();
  private RepositorySystem repositorySystem;
  private RepositorySystemSession repositorySystemSession;

  @SuppressWarnings("unused")  // used by com.github.rvesse.airline
  public PullDependencies()
  {
    hadoopExclusions = Dependencies.builder()
                                   .putAll(PROVIDED_BY_CORE_DEPENDENCIES)
                                   .putAll(SECURITY_VULNERABILITY_EXCLUSIONS)
                                   .build();
  }

  // Used for testing only
  PullDependencies(
      RepositorySystem repositorySystem,
      RepositorySystemSession repositorySystemSession,
      ExtensionsConfig extensionsConfig,
      Dependencies hadoopExclusions
  )
  {
    this.repositorySystem = repositorySystem;
    this.repositorySystemSession = repositorySystemSession;
    this.extensionsConfig = extensionsConfig;
    this.hadoopExclusions = hadoopExclusions;
  }

  private RepositorySystem getRepositorySystem()
  {
    DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
    locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
    locator.addService(TransporterFactory.class, HttpTransporterFactory.class);
    return locator.getService(RepositorySystem.class);
  }

  protected RepositorySystemSession getRepositorySystemSession()
  {
    DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();
    LocalRepository localRepo = new LocalRepository(localRepository);
    session.setLocalRepositoryManager(repositorySystem.newLocalRepositoryManager(session, localRepo));

    // Set up the proxy configuration if required
    if (useProxy) {
      Proxy proxy = new Proxy(
          proxyType,
          proxyHost,
          proxyPort,
          isBlank(proxyUsername) ? null : new AuthenticationBuilder()
              .addUsername(proxyUsername)
              .addPassword(proxyPassword)
              .build()
      );

      final DefaultProxySelector proxySelector = new DefaultProxySelector();
      proxySelector.add(proxy, null);

      session.setProxySelector(proxySelector);
    }

    return session;
  }

  protected List<RemoteRepository> getRemoteRepositories()
  {
    List<RemoteRepository> repositories = new ArrayList<>();

    if (!noDefaultRemoteRepositories) {
      repositories.add(new RemoteRepository.Builder("central", "default", DEFAULT_REMOTE_REPOSITORIES.get(0)).build());
    }

    for (String repoUrl : remoteRepositories) {
      repositories.add(new RemoteRepository.Builder(null, "default", repoUrl).build());
    }

    return repositories;
  }

  @Override
  public void run()
  {
    if (repositorySystem == null) {
      repositorySystem = getRepositorySystem();
    }

    final File extensionsDir = new File(extensionsConfig.getDirectory());
    final File hadoopDependenciesDir = new File(extensionsConfig.getHadoopDependenciesDir());

    try {
      if (clean) {
        FileUtils.deleteDirectory(extensionsDir);
        FileUtils.deleteDirectory(hadoopDependenciesDir);
      }
      FileUtils.mkdirp(extensionsDir);
      FileUtils.mkdirp(hadoopDependenciesDir);
    }
    catch (IOException e) {
      log.error(e, "Unable to clear or create extension directory at [%s]", extensionsDir);
      throw new RuntimeException(e);
    }

    log.info(
        "Start pull-deps with local repository [%s] and remote repositories [%s]",
        localRepository,
        remoteRepositories
    );

    try {
      log.info("Start downloading dependencies for extension coordinates: [%s]", coordinates);
      for (String coordinate : coordinates) {
        coordinate = coordinate.trim();
        final Artifact versionedArtifact = getArtifact(coordinate);

        File currExtensionDir = new File(extensionsDir, versionedArtifact.getArtifactId());
        createExtensionDirectory(coordinate, currExtensionDir);

        downloadExtension(versionedArtifact, currExtensionDir);
      }
      log.info("Finish downloading dependencies for extension coordinates: [%s]", coordinates);

      if (!noDefaultHadoop && hadoopCoordinates.isEmpty()) {
        hadoopCoordinates.addAll(TaskConfig.DEFAULT_DEFAULT_HADOOP_COORDINATES);
      }

      log.info("Start downloading dependencies for hadoop extension coordinates: [%s]", hadoopCoordinates);
      for (final String hadoopCoordinate : hadoopCoordinates) {
        final Artifact versionedArtifact = getArtifact(hadoopCoordinate);

        File currExtensionDir = new File(hadoopDependenciesDir, versionedArtifact.getArtifactId());
        createExtensionDirectory(hadoopCoordinate, currExtensionDir);

        // add a version folder for hadoop dependency directory
        currExtensionDir = new File(currExtensionDir, versionedArtifact.getVersion());
        createExtensionDirectory(hadoopCoordinate, currExtensionDir);

        downloadExtension(versionedArtifact, currExtensionDir, hadoopExclusions);
      }
      log.info("Finish downloading dependencies for hadoop extension coordinates: [%s]", hadoopCoordinates);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected Artifact getArtifact(String coordinate)
  {
    DefaultArtifact versionedArtifact;
    try {
      // this will throw an exception if no version is specified
      versionedArtifact = new DefaultArtifact(coordinate);
    }
    catch (IllegalArgumentException e) {
      // try appending the default version so we can specify artifacts without versions
      if (defaultVersion != null) {
        versionedArtifact = new DefaultArtifact(coordinate + ":" + defaultVersion);
      } else {
        throw e;
      }
    }
    return versionedArtifact;
  }

  /**
   * Download the extension given its maven coordinate
   *
   * @param versionedArtifact The maven artifact of the extension
   * @param toLocation        The location where this extension will be downloaded to
   */
  private void downloadExtension(Artifact versionedArtifact, File toLocation)
  {
    downloadExtension(versionedArtifact, toLocation, PROVIDED_BY_CORE_DEPENDENCIES);
  }

  private void downloadExtension(Artifact versionedArtifact, File toLocation, Dependencies exclusions)
  {
    final CollectRequest collectRequest = new CollectRequest();
    collectRequest.setRoot(new Dependency(versionedArtifact, JavaScopes.RUNTIME));

    List<RemoteRepository> repositories = getRemoteRepositories();
    for (RemoteRepository repo : repositories) {
      collectRequest.addRepository(repo);
    }

    final DependencyRequest dependencyRequest = new DependencyRequest(
        collectRequest,
        DependencyFilterUtils.andFilter(
            DependencyFilterUtils.classpathFilter(JavaScopes.RUNTIME),
            (node, parents) -> {
              String scope = node.getDependency().getScope();
              if (scope != null) {
                scope = StringUtils.toLowerCase(scope);
                if ("provided".equals(scope) || "test".equals(scope) || "system".equals(scope)) {
                  return false;
                }
              }
              if (exclusions.contain(node.getArtifact())) {
                return false;
              }

              for (DependencyNode parent : parents) {
                if (exclusions.contain(parent.getArtifact())) {
                  return false;
                }
              }

              return true;
            }
        )
    );

    try {
      log.info("Start downloading extension [%s]", versionedArtifact);
      if (repositorySystemSession == null) {
        repositorySystemSession = getRepositorySystemSession();
      }

      final DependencyResult result = repositorySystem.resolveDependencies(
          repositorySystemSession,
          dependencyRequest
      );
      final List<Artifact> artifacts = result.getArtifactResults().stream()
                                             .map(ArtifactResult::getArtifact)
                                             .collect(Collectors.toList());

      for (Artifact artifact : artifacts) {
        if (exclusions.contain(artifact)) {
          log.debug("Skipped Artifact[%s]", artifact);
        } else {
          log.info("Adding file [%s] at [%s]", artifact.getFile().getName(), toLocation.getAbsolutePath());
          org.apache.commons.io.FileUtils.copyFileToDirectory(artifact.getFile(), toLocation);
        }
      }
    }
    catch (DependencyResolutionException e) {
      if (e.getCause() instanceof ArtifactNotFoundException) {
        log.error("Artifact not found in any configured repositories: [%s]", versionedArtifact);
      } else {
        log.error(e, "Unable to resolve artifacts for [%s].", dependencyRequest);
      }
    }
    catch (IOException e) {
      log.error(e, "I/O error while processing artifact [%s].", versionedArtifact);
      throw new RuntimeException(e);
    }
    log.info("Finish downloading extension [%s]", versionedArtifact);
  }

  /**
   * Create the extension directory for a specific maven coordinate.
   * The name of this directory should be the artifactId in the coordinate
   */
  private void createExtensionDirectory(String coordinate, File atLocation)
  {
    if (atLocation.isDirectory()) {
      log.info("Directory [%s] already exists, skipping creating a directory", atLocation.getAbsolutePath());
      return;
    }

    if (!atLocation.mkdir()) {
      throw new ISE(
          "Unable to create directory at [%s] for coordinate [%s]",
          atLocation.getAbsolutePath(),
          coordinate
      );
    }
  }

  private boolean isBlank(final String toCheck)
  {
    return toCheck == null || toCheck.isEmpty();
  }

  @VisibleForTesting
  static class Dependencies
  {
    private static final String ANY_ARTIFACT_ID = "*";

    private final SetMultimap<String, String> groupIdToArtifactIds;

    private Dependencies(Builder builder)
    {
      groupIdToArtifactIds = builder.groupIdToArtifactIdsBuilder.build();
    }

    static Builder builder()
    {
      return new Builder();
    }

    boolean contain(Artifact artifact)
    {
      Set<String> artifactIds = groupIdToArtifactIds.get(artifact.getGroupId());
      return artifactIds.contains(ANY_ARTIFACT_ID) || artifactIds.contains(artifact.getArtifactId());
    }

    static final class Builder
    {
      private final ImmutableSetMultimap.Builder<String, String> groupIdToArtifactIdsBuilder =
          ImmutableSetMultimap.builder();

      private Builder()
      {
      }

      Builder putAll(Dependencies dependencies)
      {
        groupIdToArtifactIdsBuilder.putAll(dependencies.groupIdToArtifactIds);
        return this;
      }

      Builder put(String groupId)
      {
        return put(groupId, ANY_ARTIFACT_ID);
      }

      Builder put(String groupId, String artifactId)
      {
        groupIdToArtifactIdsBuilder.put(groupId, artifactId);
        return this;
      }

      Dependencies build()
      {
        return new Dependencies(this);
      }
    }
  }
}
