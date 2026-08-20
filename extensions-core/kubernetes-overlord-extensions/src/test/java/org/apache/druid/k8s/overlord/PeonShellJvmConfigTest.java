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

package org.apache.druid.k8s.overlord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Active regression coverage for the {@code peon.sh} jvm.config sourcing fix
 * (https://github.com/apache/druid/issues/18791).
 *
 * The full end-to-end test
 * ({@code KubernetesPeonJvmConfigDockerTest} under {@code embedded-tests}) is
 * {@code @Disabled} alongside its sibling K8s tests pending the
 * {@code charts.datainfra.io} chart availability issue (see #19047), so it
 * does not run in CI. This test exercises the actual sourcing logic from
 * {@code distribution/docker/peon.sh} in a sandboxed shell so a regression is
 * still caught while the heavier test stays disabled.
 */
public class PeonShellJvmConfigTest
{
  /**
   * Maven runs tests with CWD set to the module directory, so this resolves
   * to the repository's {@code distribution/docker/peon.sh}.
   */
  private static final Path PEON_SH = Paths.get("../../distribution/docker/peon.sh");

  /**
   * Exact snippet that {@code peon.sh} now contains. Kept verbatim so a drift
   * from {@code peon.sh} surfaces as a failure of
   * {@link #test_peonShFileContainsJvmConfigSourcing}.
   */
  private static final String JVM_CONFIG_SOURCING_SNIPPET =
      "if [ -f \"$SERVICE_CONF_DIR/jvm.config\" ]; then\n"
      + "    JAVA_OPTS=\"$(cat $SERVICE_CONF_DIR/jvm.config | xargs) $JAVA_OPTS\"\n"
      + "fi\n";

  /**
   * Verifies the jvm.config sourcing block prepends {@code jvm.config}
   * contents to {@code JAVA_OPTS}, leaving the existing {@code JAVA_OPTS}
   * after — so flags duplicated between the two paths still let
   * {@code JAVA_OPTS} win under OpenJDK precedence.
   */
  @Test
  public void test_peonShPrependsJvmConfigContentsToJavaOpts(@TempDir Path tempDir) throws Exception
  {
    Files.writeString(
        tempDir.resolve("jvm.config"),
        String.join(
            "\n",
            "-Xmx2g",
            "-XX:MaxDirectMemorySize=500m",
            "-Dpeon.test.marker=fromJvmConfig"
        )
    );

    final String script = JVM_CONFIG_SOURCING_SNIPPET + "echo \"$JAVA_OPTS\"\n";

    final ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", script);
    pb.environment().put("SERVICE_CONF_DIR", tempDir.toString());
    pb.environment().put("JAVA_OPTS", "-Dpeon.test.marker=fromJavaOpts -Dexisting=true");
    pb.redirectErrorStream(true);

    final Process p = pb.start();
    final String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
    assertEquals(0, p.waitFor(), "Snippet exited non-zero. Output: " + output);

    assertTrue(output.contains("-Xmx2g"), output);
    assertTrue(output.contains("-XX:MaxDirectMemorySize=500m"), output);
    assertTrue(output.contains("-Dexisting=true"), output);

    // jvm.config options are prepended; existing JAVA_OPTS stays after.
    final int xmxIdx = output.indexOf("-Xmx2g");
    final int existingIdx = output.indexOf("-Dexisting=true");
    assertTrue(
        xmxIdx >= 0 && xmxIdx < existingIdx,
        "Expected jvm.config options before existing JAVA_OPTS, got: " + output
    );

    // Both occurrences of the marker key are present, and the JAVA_OPTS value
    // appears later in the string so it wins under OpenJDK precedence.
    final int fromJvmIdx = output.indexOf("-Dpeon.test.marker=fromJvmConfig");
    final int fromJavaIdx = output.indexOf("-Dpeon.test.marker=fromJavaOpts");
    assertTrue(fromJvmIdx >= 0, "jvm.config marker missing from: " + output);
    assertTrue(fromJavaIdx >= 0, "JAVA_OPTS marker missing from: " + output);
    assertTrue(
        fromJvmIdx < fromJavaIdx,
        "Expected jvm.config marker before JAVA_OPTS marker (so JAVA_OPTS wins): " + output
    );
  }

  /**
   * The {@code -f} guard is the only deviation from {@code druid.sh}; without
   * it, peon containers without a mounted {@code jvm.config} would emit
   * {@code cat: ... No such file or directory} on every startup. Verify the
   * snippet leaves {@code JAVA_OPTS} untouched in that case.
   */
  @Test
  public void test_peonShDoesNothingWhenJvmConfigAbsent(@TempDir Path tempDir) throws Exception
  {
    final String script = JVM_CONFIG_SOURCING_SNIPPET + "echo \"$JAVA_OPTS\"\n";

    final ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", script);
    pb.environment().put("SERVICE_CONF_DIR", tempDir.toString());
    pb.environment().put("JAVA_OPTS", "-Dexisting=true");
    pb.redirectErrorStream(true);

    final Process p = pb.start();
    final String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
    assertEquals(0, p.waitFor());
    assertEquals(
        "-Dexisting=true",
        output,
        "JAVA_OPTS must be unchanged when no jvm.config is mounted"
    );
  }

  /**
   * Static guard — {@code peon.sh} must continue to contain the jvm.config
   * sourcing logic so a naive removal of the fix is caught even if the
   * snippet test above is somehow stubbed out.
   */
  @Test
  public void test_peonShFileContainsJvmConfigSourcing() throws IOException
  {
    final String content = Files.readString(PEON_SH);
    assertTrue(
        content.contains(JVM_CONFIG_SOURCING_SNIPPET),
        "peon.sh must contain the jvm.config sourcing block. See #18791."
    );
  }
}
