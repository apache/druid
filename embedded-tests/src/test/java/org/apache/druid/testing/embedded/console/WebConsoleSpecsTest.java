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

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Checks that every Playwright spec of {@code web-console/e2e-tests} is run by a {@link WebConsoleTestBase} test (the
 * specs are passed to {@link WebConsoleTestBase#runSpec} by name), and that those tests run only specs that exist.
 * It needs no cluster, so it runs with the unit tests rather than in the {@code web-console-tests} profile.
 */
public class WebConsoleSpecsTest
{
  private static final Pattern RUN_SPEC = Pattern.compile("runSpec\\(\\s*\"([^\"]+)\"");

  @Test
  public void testEverySpecIsRun() throws IOException
  {
    final Set<String> specs;
    try (Stream<Path> files = Files.list(new File(WebConsoleTestBase.webConsoleDir(), "e2e-tests").toPath())) {
      specs = files.map(file -> file.getFileName().toString())
                   .filter(name -> name.endsWith(".spec.ts"))
                   .collect(Collectors.toCollection(TreeSet::new));
    }

    final Set<String> runSpecs = new TreeSet<>();
    try (Stream<Path> files = Files.walk(Path.of("src", "test", "java"))) {
      for (Path file : files.filter(f -> f.getFileName().toString().endsWith("WebConsoleTest.java")).toList()) {
        final Matcher matcher = RUN_SPEC.matcher(Files.readString(file, StandardCharsets.UTF_8));
        while (matcher.find()) {
          runSpecs.add(matcher.group(1));
        }
      }
    }

    Assertions.assertFalse(specs.isEmpty(), "Found no specs in web-console/e2e-tests");
    Assertions.assertEquals(
        Set.of(),
        Sets.difference(specs, runSpecs),
        "Specs of web-console/e2e-tests that no *WebConsoleTest runs (with runSpec)"
    );
    Assertions.assertEquals(
        Set.of(),
        Sets.difference(runSpecs, specs),
        "Specs that a *WebConsoleTest runs, which are not in web-console/e2e-tests"
    );
  }
}
