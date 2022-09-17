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

package org.apache.druid.sql.calcite.tester;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.tester.TestSetSpec.TestCaseSpec;
import org.apache.druid.sql.calcite.tester.TestSetSpec.TestCaseSpec.Builder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Loads and parses a test case file, producing a list of test cases.
 */
public class TestCaseLoader
{
  public static TestSetSpec loadResource(String resource)
  {
    try (InputStream is = TestCaseLoader.class.getResourceAsStream(resource)) {
      if (is == null) {
        throw new IAE("Cannot open resource: " + resource);
      }
      return load(new InputStreamReader(is, StandardCharsets.UTF_8), resource);
    }
    catch (IOException e) {
      throw new IAE("Cannot close resource: " + resource);
    }
  }

  public static TestSetSpec loadFile(File file)
  {
    try {
      try (InputStream is = new FileInputStream(file)) {
        return load(new InputStreamReader(is, StandardCharsets.UTF_8), file.getName());
      }
    }
    catch (IOException e) {
      throw new IAE("Cannot open file: " + file.getAbsolutePath());
    }
  }

  public static TestSetSpec loadString(String string)
  {
    return load(new StringReader(string), "<string>");
  }

  public static TestSetSpec load(Reader reader, String label)
  {
    return new TestCaseLoader(reader, label).load();
  }

  private final TestSetSpec.Builder builder;
  private final LineNumberReader reader;
  private String pushed;
  private List<String> comment;
  private boolean eof;

  public TestCaseLoader(Reader reader, String label)
  {
    this.reader = new LineNumberReader(reader);
    builder = new TestSetSpec.Builder(label);
  }

  public String source()
  {
    return builder.source();
  }

  public TestSetSpec load()
  {
    // Ignore leading text
    loadLines();
    if (eof) {
      return builder.build();
    }
    while (loadCase()) {
      // Empty
    }
    return builder.build();
  }

  private String next()
  {
    if (pushed != null) {
      String ret = pushed;
      pushed = null;
      return ret;
    }
    try {
      return reader.readLine();
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to read query config file: " + source());
    }
  }

  private void push(String line)
  {
    pushed = line;
  }

  private Pair<String, String> parseSection(String expected, boolean expectCase)
  {
    while (true) {
      String line = next();
      if (line == null) {
        eof = true;
        return null;
      }
      if (line.startsWith("====")) {
        comment = loadLines();
        if (eof) {
          return null;
        }
        continue;
      }
      if (line.startsWith("===#") || "===".equals(line)) {
        continue;
      }
      if (!line.startsWith("=== ")) {
        throw new IAE(
            StringUtils.format(
                "[%s:%d]: Expected comments or === %s",
                builder.source(),
                reader.getLineNumber(),
                expected));
      }
      String tail = line.substring(4).trim();
      if (tail.length() == 0 || tail.charAt(0) == '#') {
        continue;
      }
      Pair<String, String> result;
      int posn = tail.indexOf(' ');
      if (posn == -1) {
        result = Pair.of(tail, "");
      } else {
        result = Pair.of(
            tail.substring(0, posn),
            tail.substring(posn + 1).trim());
      }
      if (!expectCase && "case".equalsIgnoreCase(result.lhs)) {
        push(line);
        return null;
      }
      return result;
    }
  }

  private boolean loadCase()
  {
    TestCaseSpec.Builder testCase = loadCaseSection();
    if (testCase == null) {
      return false;
    }
    while (loadSection(testCase)) {
    }
    builder.add(testCase);
    comment = null;
    return !eof;
  }

  private boolean loadSection(TestCaseSpec.Builder testCase)
  {
    int sectionStartLine = reader.getLineNumber();
    if (pushed == null) {
      sectionStartLine += 1;
    }
    Pair<String, String> parts = parseSection("<section>", false);
    if (parts == null) {
      return false;
    }
    testCase.addSection(parts.lhs, parts.rhs, sectionStartLine, loadLines());
    return !eof;
  }

  private TestCaseSpec.Builder loadCaseSection()
  {
    Pair<String, String> parts = parseSection("case", true);
    if (parts == null) {
      return null;
    }
    int startLine = reader.getLineNumber();
    if (!"case".equals(parts.lhs)) {
      throw new IAE(
          StringUtils.format(
              "[%s:%d]: First section must be case",
              source(),
              startLine
          )
      );
    }
    Builder testCase = builder.caseBuilder(startLine);
    if (comment != null) {
      testCase.comments(comment);
      comment = null;
    }
    String label = String.join("\n", loadLines()).trim();
    if (label.length() == 0) {
      label = StringUtils.format("Case at line %d", startLine);
    }
    testCase.label(label);
    return testCase;
  }

  private List<String> loadLines()
  {
    List<String> lines = new ArrayList<>();
    String line;
    while ((line = next()) != null) {
      if (line.startsWith("===")) {
        push(line);
        break;
      }
      lines.add(line);
    }
    eof = line == null;
    return lines;
  }
}
