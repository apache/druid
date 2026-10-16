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

package org.apache.druid.query.context.docs;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.context.QueryContextParameter;
import org.apache.druid.query.context.QueryContextParameters;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParameterDocumentationGeneratorTest
{
  private static final String GENERAL_DOCUMENT = "docs/querying/query-context-reference.md";
  private static final String SCAN_DOCUMENT = "docs/querying/scan-query.md";
  private static final String SQL_DOCUMENT = "docs/querying/sql-query-context.md";
  private static final String GENERAL_MARKER =
      "<!-- GENERATED QUERY CONTEXT PARAMETER: useResultLevelCache -->";
  private static final String SCAN_MARKER =
      "<!-- GENERATED QUERY CONTEXT PARAMETER: maxRowsQueuedForOrdering -->";
  private static final String SQL_TABLE = "|Parameter|Description|Default value|\n|---|---|---|\n";
  private static final String SQL_MARKER =
      "<!-- GENERATED QUERY CONTEXT PARAMETER: lane -->";

  @TempDir
  Path temporaryFolder;

  @Test
  void testGenerateAndVerify() throws IOException
  {
    writeDocuments(
        "general header\n|stale| " + GENERAL_MARKER,
        "scan header\n|stale| " + SCAN_MARKER,
        "sql header\n" + SQL_TABLE + "|stale| " + SQL_MARKER
    );

    final Path generatedOutput = temporaryFolder.resolve("generated");
    ParameterDocumentationGenerator.main(arguments("generate", generatedOutput));

    final String general = Files.readString(temporaryFolder.resolve(GENERAL_DOCUMENT), StandardCharsets.UTF_8);
    assertTrue(general.startsWith("general header\n"));
    assertTrue(general.contains("|`useResultLevelCache`| `true` |"));
    assertTrue(general.contains("|`priority`| `0` |"));
    // A parameter without a default
    assertTrue(general.contains("|`finalize`| N/A |"));
    assertTrue(general.contains(GENERAL_MARKER));

    final String scan = Files.readString(temporaryFolder.resolve(SCAN_DOCUMENT), StandardCharsets.UTF_8);
    assertTrue(scan.startsWith("scan header\n"));
    assertTrue(scan.contains("|maxRowsQueuedForOrdering|"));
    assertTrue(scan.contains("An integer in [1, 2147483647]"));
    assertTrue(scan.contains(SCAN_MARKER));

    final String sql = Files.readString(temporaryFolder.resolve(SQL_DOCUMENT), StandardCharsets.UTF_8);
    assertTrue(sql.startsWith("sql header\n"));
    assertTrue(sql.contains("|`lane`|"));
    assertTrue(sql.contains(SQL_MARKER));

    assertEquals(general, Files.readString(generatedOutput.resolve(GENERAL_DOCUMENT), StandardCharsets.UTF_8));
    assertEquals(scan, Files.readString(generatedOutput.resolve(SCAN_DOCUMENT), StandardCharsets.UTF_8));
    assertEquals(sql, Files.readString(generatedOutput.resolve(SQL_DOCUMENT), StandardCharsets.UTF_8));

    ParameterDocumentationGenerator.main(arguments("verify", temporaryFolder.resolve("verified")));
  }

  @Test
  void testVerifyRejectsStaleDocumentation() throws IOException
  {
    writeDocuments("|stale| " + GENERAL_MARKER, "|stale| " + SCAN_MARKER, SQL_TABLE + "|stale| " + SQL_MARKER);

    final ISE exception = assertThrows(
        ISE.class,
        () -> ParameterDocumentationGenerator.main(arguments("verify", temporaryFolder.resolve("generated")))
    );
    assertTrue(exception.getMessage().contains("is stale"));
  }

  @Test
  void testVerifyAcceptsCrlfLineEndings() throws IOException
  {
    writeDocuments("|stale| " + GENERAL_MARKER, "|stale| " + SCAN_MARKER, SQL_TABLE + "|stale| " + SQL_MARKER);
    ParameterDocumentationGenerator.main(arguments("generate", temporaryFolder.resolve("generated")));
    for (final String document : ParameterDocumentationGenerator.DOCUMENTS.keySet()) {
      final Path path = temporaryFolder.resolve(document);
      Files.writeString(
          path,
          StringUtils.replace(Files.readString(path, StandardCharsets.UTF_8), "\n", "\r\n"),
          StandardCharsets.UTF_8
      );
    }

    ParameterDocumentationGenerator.main(arguments("verify", temporaryFolder.resolve("verified")));
  }

  @Test
  void testGenerateRejectsMissingMarker() throws IOException
  {
    writeDocuments("no generated row", "|stale| " + SCAN_MARKER, SQL_TABLE + "|stale| " + SQL_MARKER, false);

    final ISE exception = assertThrows(
        ISE.class,
        () -> ParameterDocumentationGenerator.main(arguments("generate", temporaryFolder.resolve("generated")))
    );
    assertTrue(exception.getMessage().contains("Missing generated query context parameter markers"));
  }

  @Test
  void testTableColumnsFollowHeader() throws IOException
  {
    writeDocuments(
        "|stale| " + GENERAL_MARKER,
        "|stale| " + SCAN_MARKER,
        SQL_TABLE + "|stale| " + SQL_MARKER
        + "\n\n|Key|Description|\n|---|---|\n|stale| <!-- GENERATED QUERY CONTEXT PARAMETER: queryId -->"
    );

    ParameterDocumentationGenerator.main(arguments("generate", temporaryFolder.resolve("generated")));

    final String sql = Files.readString(temporaryFolder.resolve(SQL_DOCUMENT), StandardCharsets.UTF_8);
    assertTrue(sql.contains("|`lane`|Query lane, used to control usage limits on classes of queries."));
    assertTrue(sql.contains("|N/A| " + SQL_MARKER));
    assertTrue(sql.contains("|`queryId`|Unique identifier given to this query."));
    assertTrue(sql.contains("cancel the query.| <!-- GENERATED QUERY CONTEXT PARAMETER: queryId -->"));
  }

  @Test
  void testRejectsMarkerForNonPublicParameter() throws IOException
  {
    writeDocuments(
        "|stale| " + GENERAL_MARKER + "\n|stale| <!-- GENERATED QUERY CONTEXT PARAMETER: unknownParameter -->",
        "|stale| " + SCAN_MARKER,
        SQL_TABLE + "|stale| " + SQL_MARKER
    );

    final ISE exception = assertThrows(
        ISE.class,
        () -> ParameterDocumentationGenerator.main(arguments("generate", temporaryFolder.resolve("generated")))
    );
    assertTrue(exception.getMessage().contains("does not name a public query context parameter"));
  }

  @Test
  void testSkipDoesNotReadDocuments() throws IOException
  {
    final Path generatedOutput = temporaryFolder.resolve("generated");
    ParameterDocumentationGenerator.main(arguments("skip", generatedOutput));
    assertFalse(Files.exists(generatedOutput));
  }

  @Test
  void testRejectsUnexpectedArguments()
  {
    final ISE exception = assertThrows(ISE.class, () -> ParameterDocumentationGenerator.main(new String[0]));
    assertTrue(exception.getMessage().contains("Expected arguments"));
  }

  @Test
  void testEscapesTableCell()
  {
    assertEquals("a \\| b", ParameterDocumentationGenerator.escapeTableCell("a | b"));
  }

  private String[] arguments(final String mode, final Path generatedOutput)
  {
    return new String[]{temporaryFolder.toString(), mode, generatedOutput.toString()};
  }

  private void writeDocuments(final String general, final String scan, final String sql) throws IOException
  {
    writeDocuments(general, scan, sql, true);
  }

  /**
   * Writes the three documents under test, and an empty file for every other managed document. When
   * {@code addMissingMarkers} is set, markers for public parameters that none of the three documents mention are
   * appended to the general document so that the generator's completeness check passes.
   */
  private void writeDocuments(
      final String general,
      final String scan,
      final String sql,
      final boolean addMissingMarkers
  ) throws IOException
  {
    for (final String document : ParameterDocumentationGenerator.DOCUMENTS.keySet()) {
      writeDocument(document, "");
    }
    writeDocument(GENERAL_DOCUMENT, addMissingMarkers ? addMissingMarkers(general, general + scan + sql) : general);
    writeDocument(SCAN_DOCUMENT, scan);
    writeDocument(SQL_DOCUMENT, sql);
  }

  private void writeDocument(final String document, final String content) throws IOException
  {
    final Path path = temporaryFolder.resolve(document);
    Files.createDirectories(path.getParent());
    Files.writeString(path, content, StandardCharsets.UTF_8);
  }

  private String addMissingMarkers(final String document, final String allDocuments)
  {
    final StringBuilder output = new StringBuilder(document);
    for (final QueryContextParameter<?> parameter : QueryContextParameters.ALL.get().values()) {
      final String marker = "<!-- GENERATED QUERY CONTEXT PARAMETER: " + parameter.getName() + " -->";
      if (parameter.getVisibility() == QueryContextParameter.Visibility.PUBLIC && !allDocuments.contains(marker)) {
        output.append("\n|stale| ").append(marker);
      }
    }
    return output.toString();
  }
}
