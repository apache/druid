/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.cli;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.druid.guice.ExtensionsConfig;
import io.druid.indexing.common.config.TaskConfig;
import io.tesla.aether.Repository;
import io.tesla.aether.TeslaAether;
import io.tesla.aether.internal.DefaultTeslaAether;
import org.apache.commons.io.FileUtils;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.DependencyFilter;
import org.eclipse.aether.graph.DependencyNode;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.util.artifact.JavaScopes;
import org.eclipse.aether.util.filter.DependencyFilterUtils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;


@Command(
    name = "pull-deps",
    description = "Pull down dependencies to the local repository specified by druid.extensions.localRepository, extensions directory specified by druid.extensions.extensionsDir and hadoop depenencies directory specified by druid.extensions.hadoopDependenciesDir"
)
public class PullDependencies implements Runnable
{
  private static final Logger log = new Logger(PullDependencies.class);

  private static final Set<String> exclusions = Sets.newHashSet(
      "io.druid",
      "com.metamx.druid"
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
      description = "Don't pull down the default hadoop coordinate, i.e., org.apache.hadoop:hadoop-client:2.3.0. If `-h` option is supplied, then default hadoop coordinate will not be downloaded.",
      required = false)
  public boolean noDefaultHadoop = false;

  @Option(
      name = "--clean",
      title = "Remove exisiting extension and hadoop dependencies directories before pulling down dependencies.",
      required = false)
  public boolean clean = false;

  @Option(
      name = {"-l", "--localRepository"},
      title = "A local repostiry that Maven will use to put downloaded files. Then pull-deps will lay these files out into the extensions directory as needed.",
      required = false
  )
  public String localRepository = String.format("%s/%s", System.getProperty("user.home"), ".m2/repository");

  @Option(
      name = {"-r", "--remoteRepositories"},
      title = "A JSON Array list of remote repositories to load dependencies from.",
      required = false
  )
  List<String> remoteRepositories = ImmutableList.of(
      "https://repo1.maven.org/maven2/",
      "https://metamx.artifactoryonline.com/metamx/pub-libs-releases-local"
  );

  @Option(
      name = {"-d", "--defaultVersion"},
      title = "Version to use for extension artifacts without version information.",
      required = false
  )
  public String defaultVersion = PullDependencies.class.getPackage().getImplementationVersion();

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

    if (clean) {
      try {
        FileUtils.deleteDirectory(extensionsDir);
        FileUtils.deleteDirectory(hadoopDependenciesDir);
      }
      catch (IOException e) {
        log.error("Unable to clear extension directory at [%s]", extensionsConfig.getDirectory());
        throw Throwables.propagate(e);
      }
    }

    createRootExtensionsDirectory(extensionsDir);
    createRootExtensionsDirectory(hadoopDependenciesDir);

    try {
      log.info("Start downloading dependencies for extension coordinates: [%s]", coordinates);
      for (final String coordinate : coordinates) {
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

    List<String> remoteUriList = remoteRepositories;

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
      return new DefaultTeslaAether(
          localRepository,
          remoteRepositories.toArray(new Repository[remoteRepositories.size()])
      );
    }

    PrintStream oldOut = System.out;
    try {
      System.setOut(
          new PrintStream(
              new OutputStream()
              {
                @Override
                public void write(int b) throws IOException
                {

                }

                @Override
                public void write(byte[] b) throws IOException
                {

                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException
                {

                }
              }
              , false, StringUtils.UTF8_STRING
          )
      );
      return new DefaultTeslaAether(
          localRepository,
          remoteRepositories.toArray(new Repository[remoteRepositories.size()])
      );
    }
    catch (UnsupportedEncodingException e) {
      // should never happen
      throw new IllegalStateException(e);
    }
    finally {
      System.setOut(oldOut);
    }
  }

  private void createRootExtensionsDirectory(File atLocation)
  {
    if (!atLocation.mkdirs()) {
      throw new ISE(
          String.format(
              "Unable to create extensions directory at [%s]",
              atLocation.getAbsolutePath()
          )
      );
    }
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
          String.format(
              "Unable to create directory at [%s] for coordinate [%s]",
              atLocation.getAbsolutePath(),
              coordinate
          )
      );
    }
  }
}
