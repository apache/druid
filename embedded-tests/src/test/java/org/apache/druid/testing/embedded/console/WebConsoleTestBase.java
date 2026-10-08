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

package org.apache.druid.testing.embedded.console;

import org.apache.druid.guice.SleepModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchModule;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchModule;
import org.apache.druid.query.aggregation.datasketches.theta.SketchModule;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Base class for the web console end-to-end tests: a cluster with a Router (which serves the web console), on which
 * the tests run the Playwright specs of {@code web-console/e2e-tests}.
 * <p>
 * Steps to write a test:
 * <ul>
 * <li>Write the Playwright spec in {@code web-console/e2e-tests}.</li>
 * <li>Write a {@code *WebConsoleTest} class that extends this class. Add the resources the spec needs (S3, Kafka,
 * etc.) in {@link #addResources}, and the extensions and properties in {@link #configureCluster}.</li>
 * <li>Write a {@code @Test} method that calls {@link #runSpec}, passing the settings of the resources to the spec
 * through environment variables.</li>
 * </ul>
 * The tests run only with the {@code web-console-tests} profile:
 * <pre>
 * mvn -pl embedded-tests verify -Pweb-console-tests -Dit.test=CoreWebConsoleTest
 * </pre>
 * With {@code -Dweb.console.keepAlive=true}, {@link #runSpec} doesn't run the spec. It prints the command to run it
 * and waits, keeping the cluster up, so that the spec can be run (and debugged) from {@code web-console} against it.
 * With {@code -Dweb.console.port}, the specs run against the console on that port (such as the dev server of
 * {@code npm start}, which proxies to the Router on port 8888) rather than the console that the Router serves.
 */
@Tag("web-console")
public abstract class WebConsoleTestBase extends EmbeddedClusterTestBase
{
  private static final Logger log = new Logger(WebConsoleTestBase.class);

  private static final long SPEC_TIMEOUT_MINUTES = 15;
  private static final Pattern NOT_FILE_NAME_CHARS = Pattern.compile("[^A-Za-z0-9.-]");

  protected final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  protected final EmbeddedOverlord overlord = new EmbeddedOverlord();
  protected final EmbeddedIndexer indexer = new EmbeddedIndexer();
  protected final EmbeddedHistorical historical = new EmbeddedHistorical();
  protected final EmbeddedBroker broker = new EmbeddedBroker();
  protected final EmbeddedRouter router = new EmbeddedRouter();

  @Override
  protected final EmbeddedDruidCluster createCluster()
  {
    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper();
    // Resources start in the order they are added, and must start before the Druid servers that use them
    addResources(cluster);

    // SleepModule has the sleep() function, for queries that run long enough to be canceled
    cluster.addExtensions(SketchModule.class, HllSketchModule.class, DoublesSketchModule.class, SleepModule.class)
           .addCommonProperty("druid.msq.dart.enabled", "true")
           .addCommonProperty("druid.sql.planner.enableSysQueriesTable", "true")
           // The console waits for the cluster state to change, so make it change quickly
           .addCommonProperty("druid.manager.segments.pollDuration", "PT1S")
           .addServer(coordinator.addProperty("druid.coordinator.period", "PT1S"))
           .addServer(overlord)
           // Enough task slots for an MSQ query (a controller and a worker) next to an ingestion task
           .addServer(indexer.addProperty("druid.worker.capacity", "4").setServerMemory(1_000_000_000L))
           .addServer(historical)
           .addServer(broker)
           .addServer(router);

    configureCluster(cluster);
    return cluster;
  }

  /**
   * Adds the resources (such as an S3 container) that the specs of this test need.
   */
  protected void addResources(EmbeddedDruidCluster cluster)
  {
  }

  /**
   * Adds the extensions and properties that the specs of this test need.
   */
  protected void configureCluster(EmbeddedDruidCluster cluster)
  {
  }

  /**
   * Runs the Playwright spec (a file name or a pattern of {@code web-console/e2e-tests}) against this cluster and
   * fails if it fails.
   *
   * @param env environment variables to pass to the spec, such as the settings of the resources
   */
  protected void runSpec(String spec, Map<String, String> env) throws Exception
  {
    final File webConsoleDir = webConsoleDir();
    final Map<String, String> specEnv = new HashMap<>(env);
    specEnv.put(
        "DRUID_E2E_TEST_UNIFIED_CONSOLE_PORT",
        System.getProperty("web.console.port", String.valueOf(router.bindings().selfNode().getPlaintextPort()))
    );
    // Each spec has its own output directory, as Playwright empties it on every run
    final String outputDir = "test-results/" + NOT_FILE_NAME_CHARS.matcher(spec).replaceAll("_");
    final List<String> command = List.of("npx", "playwright", "test", spec, "--output", outputDir);

    if (Boolean.getBoolean("web.console.keepAlive")) {
      keepAlive(webConsoleDir, specEnv, command);
      return;
    }

    log.info("Running spec[%s] in [%s] with env[%s].", spec, webConsoleDir, specEnv);
    final ProcessBuilder processBuilder = new ProcessBuilder(command).directory(webConsoleDir)
                                                                     .redirectErrorStream(true);
    processBuilder.environment().putAll(specEnv);
    final Process process = processBuilder.start();

    // Log the output as it comes (so that a hung spec can be seen) and keep it for the failure message
    final List<String> output = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8)
    )) {
      String line;
      while ((line = reader.readLine()) != null) {
        log.info("[playwright] %s", line);
        output.add(line);
      }
    }

    if (!process.waitFor(SPEC_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
      process.destroyForcibly();
      Assertions.fail(StringUtils.format("Spec[%s] did not finish in %d minutes", spec, SPEC_TIMEOUT_MINUTES));
    }
    Assertions.assertEquals(
        0,
        process.exitValue(),
        StringUtils.format(
            "Spec[%s] failed (its screenshots and traces are in [%s]):\n%s",
            spec,
            new File(webConsoleDir, outputDir),
            String.join("\n", output)
        )
    );
  }

  /**
   * The {@code web-console} directory of this checkout.
   */
  protected static File webConsoleDir() throws IOException
  {
    return new File(System.getProperty("web.console.dir", "../web-console")).getCanonicalFile();
  }

  private void keepAlive(File webConsoleDir, Map<String, String> specEnv, List<String> command)
      throws InterruptedException
  {
    final String envString = specEnv.entrySet()
                                    .stream()
                                    .map(entry -> entry.getKey() + "=" + entry.getValue())
                                    .collect(Collectors.joining(" "));
    // Printed to stderr as well as logged, to stand out from the logs of the cluster
    System.err.println(
        StringUtils.format(
            "%n%n==== The cluster is up, with the console at [%s]. Run the spec with:%n"
            + "cd %s && %s %s%n"
            + "==== Stop this process to stop the cluster.%n%n",
            getServerUrl(router),
            webConsoleDir,
            envString,
            String.join(" ", command)
        )
    );
    Thread.sleep(Long.MAX_VALUE);
  }
}
