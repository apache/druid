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

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.druid.guice.ExtensionsConfig;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.tesla.aether.Repository;
import io.tesla.aether.TeslaAether;
import io.tesla.aether.guice.RepositorySystemSessionProvider;
import io.tesla.aether.internal.DefaultTeslaAether;
import org.apache.commons.io.FileUtils;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.DependencyFilter;
import org.eclipse.aether.graph.DependencyNode;
import org.eclipse.aether.repository.Authentication;
import org.eclipse.aether.repository.Proxy;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.util.artifact.JavaScopes;
import org.eclipse.aether.util.filter.DependencyFilterUtils;
import org.eclipse.aether.util.repository.AuthenticationBuilder;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
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

  private static final Set<String> exclusions = Sets.newHashSet(
      /*

      // It is possible that extensions will pull down a lot of jars that are either
      // duplicates OR conflict with druid jars. In that case, there are two problems that arise
      //
      // 1. Large quantity of jars are passed around to things like hadoop when they are not needed (and should not be included)
      // 2. Classpath priority becomes "mostly correct" and attempted to enforced correctly, but not fully tested
      //
      // These jar groups should be included by druid and *not* pulled down in extensions
      // Note to future developers: This list is hand-crafted and will probably be out of date in the future
      // A good way to know where to look for errant dependencies is to compare the lib/ directory in the distribution
      // tarball with the jars included in the extension directories.
      //
      // This list is best-effort, and might still pull down more than desired.
      //
      // A simple example is that if an extension's dependency uses some-library-123.jar,
      // druid uses some-library-456.jar, and hadoop uses some-library-666.jar, then we probably want to use some-library-456.jar,
      // so don't pull down some-library-123.jar, and ask hadoop to load some-library-456.jar.
      //
      // In the case where some-library is NOT on this list, both some-library-456.jar and some-library-123.jar will be
      // on the class path and propagated around the system. Most places TRY to make sure some-library-456.jar has
      // precedence, but it is easy for this assumption to be violated and for the precedence of some-library-456.jar,
      // some-library-123.jar and some-library-456.jar to not be properly defined.
      //
      // As of this writing there are no special unit tests for classloader issues and library version conflicts.
      //
      // Different tasks which are classloader sensitive attempt to maintain a sane order for loading libraries in the
      // classloader, but it is always possible that something didn't load in the right order. Also we don't want to be
      // throwing around a ton of jars we don't need to.
      //
      // Here is a list of dependencies extensions should probably exclude.
      //
      // Conflicts can be discovered using the following command on the distribution tarball:
      //    `find lib -iname *.jar | cut -d / -f 2 | sed -e 's/-[0-9]\.[0-9]/@/' | cut -f 1 -d @ | sort | uniq | xargs -I {} find extensions -name "*{}*.jar" | sort`

      "io.druid",
      "com.metamx.druid",
      "asm",
      "org.ow2.asm",
      "org.jboss.netty",
      "com.google.guava",
      "com.google.code.findbugs",
      "com.google.protobuf",
      "com.esotericsoftware.minlog",
      "log4j",
      "org.slf4j",
      "commons-logging",
      "org.eclipse.jetty",
      "org.mortbay.jetty",
      "com.sun.jersey",
      "com.sun.jersey.contribs",
      "common-beanutils",
      "commons-codec",
      "commons-lang",
      "commons-cli",
      "commons-io",
      "javax.activation",
      "org.apache.httpcomponents",
      "org.apache.zookeeper",
      "org.codehaus.jackson",
      "com.fasterxml.jackson",
      "com.fasterxml.jackson.core",
      "com.fasterxml.jackson.dataformat",
      "com.fasterxml.jackson.datatype",
      "org.roaringbitmap",
      "net.java.dev.jets3t"
      */
  );

  private static final List<String> DEFAULT_REMOTE_REPOSITORIES = ImmutableList.of(
      "https://repo1.maven.org/maven2/"
  );

  private TeslaAether aether;

  @Inject
  public ExtensionsConfig extensionsConfig;

  @Option(
      name = {"-c", "--coordinate"},
      title = "coordinate",
      description = "Extension coordinate to pull down, followed by a maven coordinate, e.g. io.druid.extensions:mysql-metadata-storage",
      required = false)
  public List<String> coordinates = Lists.newArrayList();

  @Option(
      name = {"-h", "--hadoop-coordinate"},
      title = "hadoop coordinate",
      description = "Hadoop dependency to pull down, followed by a maven coordinate, e.g. org.apache.hadoop:hadoop-client:2.4.0",
      required = false)
  public List<String> hadoopCoordinates = Lists.newArrayList();

  @Option(
      name = "--no-default-hadoop",
      description = "Don't pull down the default hadoop coordinate, i.e., org.apache.hadoop:hadoop-client:2.8.3. If `-h` option is supplied, then default hadoop coordinate will not be downloaded.",
      required = false)
  public boolean noDefaultHadoop = false;

  @Option(
      name = "--clean",
      title = "Remove exisiting extension and hadoop dependencies directories before pulling down dependencies.",
      required = false)
  public boolean clean = false;

  @Option(
      name = {"-l", "--localRepository"},
      title = "A local repository that Maven will use to put downloaded files. Then pull-deps will lay these files out into the extensions directory as needed.",
      required = false
  )
  public String localRepository = StringUtils.format("%s/%s", System.getProperty("user.home"), ".m2/repository");

  @Option(
      name = {"-r", "--remoteRepository"},
      title = "Add a remote repository. Unless --no-default-remote-repositories is provided, these will be used after https://repo1.maven.org/maven2/",
      required = false
  )
  List<String> remoteRepositories = Lists.newArrayList();

  @Option(
      name = "--no-default-remote-repositories",
      description = "Don't use the default remote repositories, only use the repositories provided directly via --remoteRepository",
      required = false)
  public boolean noDefaultRemoteRepositories = false;

  @Option(
      name = {"-d", "--defaultVersion"},
      title = "Version to use for extension artifacts without version information.",
      required = false
  )
  public String defaultVersion = PullDependencies.class.getPackage().getImplementationVersion();

  @Option(
      name = {"--use-proxy"},
      title = "Use http/https proxy to pull dependencies.",
      required = false
  )
  public boolean useProxy = false;

  @Option(
      name = {"--proxy-type"},
      title = "The proxy type, should be either http or https",
      required = false
  )
  public String proxyType = "https";

  @Option(
      name = {"--proxy-host"},
      title = "The proxy host",
      required = false
  )
  public String proxyHost = "";

  @Option(
      name = {"--proxy-port"},
      title = "The proxy port",
      required = false
  )
  public int proxyPort = -1;

  @Option(
      name = {"--proxy-username"},
      title = "The proxy username",
      required = false
  )
  public String proxyUsername = "";

  @Option(
      name = {"--proxy-password"},
      title = "The proxy password",
      required = false
  )
  public String proxyPassword = "";

  public PullDependencies()
  {
  }

  // Used for testing only
  PullDependencies(TeslaAether aether, ExtensionsConfig extensionsConfig)
  {
    this.aether = aether;
    this.extensionsConfig = extensionsConfig;
  }

  @Override
  public void run()
  {
    if (aether == null) {
      aether = getAetherClient();
    }

    final File extensionsDir = new File(extensionsConfig.getDirectory());
    final File hadoopDependenciesDir = new File(extensionsConfig.getHadoopDependenciesDir());

    try {
      if (clean) {
        FileUtils.deleteDirectory(extensionsDir);
        FileUtils.deleteDirectory(hadoopDependenciesDir);
      }
      FileUtils.forceMkdir(extensionsDir);
      FileUtils.forceMkdir(hadoopDependenciesDir);
    }
    catch (IOException e) {
      log.error(e, "Unable to clear or create extension directory at [%s]", extensionsDir);
      throw Throwables.propagate(e);
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

        downloadExtension(versionedArtifact, currExtensionDir);
      }
      log.info("Finish downloading dependencies for hadoop extension coordinates: [%s]", hadoopCoordinates);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private Artifact getArtifact(String coordinate)
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
    final CollectRequest collectRequest = new CollectRequest();
    collectRequest.setRoot(new Dependency(versionedArtifact, JavaScopes.RUNTIME));
    final DependencyRequest dependencyRequest = new DependencyRequest(
        collectRequest,
        DependencyFilterUtils.andFilter(
            DependencyFilterUtils.classpathFilter(JavaScopes.RUNTIME),
            new DependencyFilter()
            {
              @Override
              public boolean accept(DependencyNode node, List<DependencyNode> parents)
              {
                String scope = node.getDependency().getScope();
                if (scope != null) {
                  scope = StringUtils.toLowerCase(scope);
                  if (scope.equals("provided")) {
                    return false;
                  }
                  if (scope.equals("test")) {
                    return false;
                  }
                  if (scope.equals("system")) {
                    return false;
                  }
                }
                if (accept(node.getArtifact())) {
                  return false;
                }

                for (DependencyNode parent : parents) {
                  if (accept(parent.getArtifact())) {
                    return false;
                  }
                }

                return true;
              }

              private boolean accept(final Artifact artifact)
              {
                return exclusions.contains(artifact.getGroupId());
              }
            }
        )
    );

    try {
      log.info("Start downloading extension [%s]", versionedArtifact);
      final List<Artifact> artifacts = aether.resolveArtifacts(dependencyRequest);

      for (Artifact artifact : artifacts) {
        if (!exclusions.contains(artifact.getGroupId())) {
          log.info("Adding file [%s] at [%s]", artifact.getFile().getName(), toLocation.getAbsolutePath());
          FileUtils.copyFileToDirectory(artifact.getFile(), toLocation);
        } else {
          log.debug("Skipped Artifact[%s]", artifact);
        }
      }
    }
    catch (Exception e) {
      log.error(e, "Unable to resolve artifacts for [%s].", dependencyRequest);
      throw Throwables.propagate(e);
    }
    log.info("Finish downloading extension [%s]", versionedArtifact);
  }

  private DefaultTeslaAether getAetherClient()
  {
    /*
    DefaultTeslaAether logs a bunch of stuff to System.out, which is annoying.  We choose to disable that
    unless debug logging is turned on.  "Disabling" it, however, is kinda bass-ackwards.  We copy out a reference
    to the current System.out, and set System.out to a noop output stream.  Then after DefaultTeslaAether has pulled
    The reference we swap things back.

    This has implications for other things that are running in parallel to this.  Namely, if anything else also grabs
    a reference to System.out or tries to log to it while we have things adjusted like this, then they will also log
    to nothingness.  Fortunately, the code that calls this is single-threaded and shouldn't hopefully be running
    alongside anything else that's grabbing System.out.  But who knows.
    */

    final List<String> remoteUriList = Lists.newArrayList();
    if (!noDefaultRemoteRepositories) {
      remoteUriList.addAll(DEFAULT_REMOTE_REPOSITORIES);
    }
    remoteUriList.addAll(remoteRepositories);

    List<Repository> remoteRepositories = Lists.newArrayList();
    for (String uri : remoteUriList) {
      try {
        URI u = new URI(uri);
        Repository r = new Repository(uri);

        if (u.getUserInfo() != null) {
          String[] auth = u.getUserInfo().split(":", 2);
          if (auth.length == 2) {
            r.setUsername(auth[0]);
            r.setPassword(auth[1]);
          } else {
            log.warn(
                "Invalid credentials in repository URI, expecting [<user>:<password>], got [%s] for [%s]",
                u.getUserInfo(),
                uri
            );
          }
        }
        remoteRepositories.add(r);
      }
      catch (URISyntaxException e) {
        throw Throwables.propagate(e);
      }
    }

    if (log.isTraceEnabled() || log.isDebugEnabled()) {
      return createTeslaAether(remoteRepositories);
    }

    PrintStream oldOut = System.out;
    try {
      System.setOut(
          new PrintStream(
              new OutputStream()
              {
                @Override
                public void write(int b)
                {

                }

                @Override
                public void write(byte[] b)
                {

                }

                @Override
                public void write(byte[] b, int off, int len)
                {

                }
              },
              false,
              StringUtils.UTF8_STRING
          )
      );
      return createTeslaAether(remoteRepositories);
    }
    catch (UnsupportedEncodingException e) {
      // should never happen
      throw new IllegalStateException(e);
    }
    finally {
      System.setOut(oldOut);
    }
  }

  private DefaultTeslaAether createTeslaAether(List<Repository> remoteRepositories)
  {
    if (!useProxy) {
      return new DefaultTeslaAether(
          localRepository,
          remoteRepositories.toArray(new Repository[remoteRepositories.size()])
      );
    }

    if (!StringUtils.toLowerCase(proxyType).equals(Proxy.TYPE_HTTP) && !StringUtils.toLowerCase(proxyType).equals(Proxy.TYPE_HTTPS)) {
      throw new IllegalArgumentException("invalid proxy type: " + proxyType);
    }

    RepositorySystemSession repositorySystemSession = new RepositorySystemSessionProvider(new File(localRepository)).get();
    List<RemoteRepository> rl = remoteRepositories.stream().map(r -> {
      RemoteRepository.Builder builder = new RemoteRepository.Builder(r.getId(), "default", r.getUrl());
      if (r.getUsername() != null && r.getPassword() != null) {
        Authentication auth = new AuthenticationBuilder().addUsername(r.getUsername()).addPassword(r.getPassword()).build();
        builder.setAuthentication(auth);
      }

      final Authentication proxyAuth;
      if (Strings.isNullOrEmpty(proxyUsername)) {
        proxyAuth = null;
      } else {
        proxyAuth = new AuthenticationBuilder().addUsername(proxyUsername).addPassword(proxyPassword).build();
      }
      builder.setProxy(new Proxy(proxyType, proxyHost, proxyPort, proxyAuth));
      return builder.build();
    }).collect(Collectors.toList());
    return new DefaultTeslaAether(rl, repositorySystemSession);
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
}
