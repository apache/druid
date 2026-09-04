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
import org.apache.druid.query.context.QueryContextParameter;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.context.docs.ParameterDocumentation.Query;
import org.apache.druid.query.context.docs.ParameterDocumentation.QueryType;
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
  private static final String SQL_MARKER =
      "<!-- GENERATED QUERY CONTEXT PARAMETER: sqlQueryId -->";

  @TempDir
  Path temporaryFolder;

  @Test
  void testGenerateAndVerify() throws IOException
  {
    writeDocuments(
        "general header\n|stale| " + GENERAL_MARKER,
        "scan header\n|stale| " + SCAN_MARKER,
        "sql header\n|stale| " + SQL_MARKER
    );

    final Path generatedOutput = temporaryFolder.resolve("generated");
    ParameterDocumentationGenerator.main(arguments("generate", generatedOutput));

    final String general = Files.readString(temporaryFolder.resolve(GENERAL_DOCUMENT), StandardCharsets.UTF_8);
    assertTrue(general.startsWith("general header\n"));
    assertTrue(general.contains("|`useResultLevelCache`| `true` |"));
    assertTrue(
        general.contains(
            "|`priority`| The default priority is one of the following: <ul><li>Value of `priority` in the query context, if set"
        )
    );
    assertTrue(general.contains(GENERAL_MARKER));

    final String scan = Files.readString(temporaryFolder.resolve(SCAN_DOCUMENT), StandardCharsets.UTF_8);
    assertTrue(scan.startsWith("scan header\n"));
    assertTrue(scan.contains("|maxRowsQueuedForOrdering|"));
    assertTrue(scan.contains("An integer in [1, 2147483647]"));
    assertTrue(scan.contains(SCAN_MARKER));

    final String sql = Files.readString(temporaryFolder.resolve(SQL_DOCUMENT), StandardCharsets.UTF_8);
    assertTrue(sql.startsWith("sql header\n"));
    assertTrue(sql.contains("|`sqlQueryId`|"));
    assertTrue(sql.contains(SQL_MARKER));
    assertFalse(sql.contains("dartQueryId"));

    assertEquals(general, Files.readString(generatedOutput.resolve(GENERAL_DOCUMENT), StandardCharsets.UTF_8));
    assertEquals(scan, Files.readString(generatedOutput.resolve(SCAN_DOCUMENT), StandardCharsets.UTF_8));
    assertEquals(sql, Files.readString(generatedOutput.resolve(SQL_DOCUMENT), StandardCharsets.UTF_8));

    ParameterDocumentationGenerator.main(arguments("verify", temporaryFolder.resolve("verified")));
  }

  @Test
  void testVerifyRejectsStaleDocumentation() throws IOException
  {
    writeDocuments("|stale| " + GENERAL_MARKER, "|stale| " + SCAN_MARKER, "|stale| " + SQL_MARKER);

    final ISE exception = assertThrows(
        ISE.class,
        () -> ParameterDocumentationGenerator.main(arguments("verify", temporaryFolder.resolve("generated")))
    );
    assertTrue(exception.getMessage().contains("is stale"));
  }

  @Test
  void testGenerateRejectsMissingMarker() throws IOException
  {
    writeDocuments("no generated row", "|stale| " + SCAN_MARKER, "|stale| " + SQL_MARKER, false);

    final ISE exception = assertThrows(
        ISE.class,
        () -> ParameterDocumentationGenerator.main(arguments("generate", temporaryFolder.resolve("generated")))
    );
    assertTrue(exception.getMessage().contains("Missing generated query context parameter markers"));
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
  void testNormalizesTableCell()
  {
    final String description = String.join(
        "",
        " Description with ",
        "\\",
        "\n",
        " wrapped lines ",
        "\n and   a continuation marker. "
    );

    assertEquals(
        "Description with wrapped lines and a continuation marker.",
        ParameterDocumentationGenerator.normalizeTableCell(description)
    );
  }

  private String[] arguments(final String mode, final Path generatedOutput)
  {
    return new String[]{temporaryFolder.toString(), mode, generatedOutput.toString()};
  }

  private void writeDocuments(final String general, final String scan, final String sql) throws IOException
  {
    writeDocuments(general, scan, sql, true);
  }

  private void writeDocuments(
      final String general,
      final String scan,
      final String sql,
      final boolean addMissingMarkers
  ) throws IOException
  {
    final Path generalPath = temporaryFolder.resolve(GENERAL_DOCUMENT);
    final Path scanPath = temporaryFolder.resolve(SCAN_DOCUMENT);
    final Path sqlPath = temporaryFolder.resolve(SQL_DOCUMENT);
    Files.createDirectories(generalPath.getParent());
    Files.writeString(
        generalPath,
        addMissingMarkers ? addMissingMarkers(general, GENERAL_DOCUMENT) : general,
        StandardCharsets.UTF_8
    );
    Files.writeString(
        scanPath,
        addMissingMarkers ? addMissingMarkers(scan, SCAN_DOCUMENT) : scan,
        StandardCharsets.UTF_8
    );
    Files.writeString(
        sqlPath,
        addMissingMarkers ? addMissingMarkers(sql, SQL_DOCUMENT) : sql,
        StandardCharsets.UTF_8
    );
  }

  private String addMissingMarkers(final String document, final String documentPath)
  {
    final StringBuilder output = new StringBuilder(document);
    for (final QueryContextParameter<?> parameter : QueryContextParameters.BY_NAME.values()) {
      if (parameter.isInternal()) {
        continue;
      }
      final ParameterDocumentation docs = parameter.getDocumentation().orElse(null);
      if (docs == null) {
        continue;
      }
      final String generatedDocument;
      if (docs.getQueries().contains(Query.SQL) && !docs.getQueries().contains(Query.JSON)) {
        generatedDocument = SQL_DOCUMENT;
      } else if (docs.getQueryTypes().contains(QueryType.SCAN)) {
        generatedDocument = SCAN_DOCUMENT;
      } else {
        generatedDocument = GENERAL_DOCUMENT;
      }
      if (generatedDocument.equals(documentPath)) {
        final String marker = "<!-- GENERATED QUERY CONTEXT PARAMETER: " + parameter.getName() + " -->";
        if (!document.contains(marker)) {
          output.append("\n|stale| ").append(marker);
        }
      }
    }
    return output.toString();
  }
}
