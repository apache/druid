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
  private static final String REFERENCE =
      "intro\n\n## General\n\n" + parameters("*")
      + "\n## GroupBy\n\n" + parameters("queryTypes=GROUP_BY")
      + "\n## Learn more\n";
  private static final String SCAN_DOCUMENT = "docs/querying/scan-query.md";
  private static final String MSQ_DOCUMENT = "docs/multi-stage-query/reference.md";

  @TempDir
  Path temporaryFolder;

  @Test
  void testGenerateAndVerify() throws IOException
  {
    writeDocuments(REFERENCE);
    writeDocument(SCAN_DOCUMENT, "before\n" + table("queryTypes=SCAN") + "after\n");
    writeDocument(MSQ_DOCUMENT, "before\n" + table("engines~MSQ engines!~NATIVE") + "after\n");

    final Path generatedOutput = temporaryFolder.resolve("generated");
    ParameterDocumentationGenerator.main(arguments("generate", generatedOutput));

    final String reference = read(ParameterDocumentationGenerator.REFERENCE_DOCUMENT);
    // Headings and text around the markers are left alone
    assertTrue(reference.startsWith(
        "intro\n\n## General\n\n<!-- BEGIN GENERATED QUERY CONTEXT PARAMETERS: * -->\n\n### `"
    ));
    assertTrue(reference.endsWith("<!-- END GENERATED QUERY CONTEXT PARAMETERS -->\n\n## Learn more\n"));
    assertFalse(reference.contains("stale"));
    assertTrue(reference.contains("### `useResultLevelCache`\n\n| | |\n|---|---|\n|**Description**|"));
    assertTrue(reference.contains("|**Type**|Boolean|\n|**Default**|`true`|"));
    // A parameter without a default
    assertTrue(reference.contains("|**Default**|N/A|"));
    assertTrue(reference.contains("|**Type**|Integer in [1, 2147483647]|"));
    assertTrue(reference.contains("|**Type**|Enum (`false`, `true`, `force`)|"));
    // Each parameter appears once, under the marker whose filter selects it, sorted by name within the marker
    final int groupBy = reference.indexOf("## GroupBy");
    assertTrue(reference.indexOf("### `maxOnDiskStorage`\n") > groupBy);
    assertTrue(reference.indexOf("### `bySegment`\n") < reference.indexOf("### `finalize`\n"));
    assertTrue(reference.indexOf("### `finalize`\n") < groupBy);
    assertEquals(reference.indexOf("### `sqlQueryId`\n"), reference.lastIndexOf("### `sqlQueryId`\n"));
    // Non-public parameters are left out
    assertFalse(reference.contains("optimizeAggregators"));
    assertFalse(reference.contains("prePlanned"));

    final String scan = read(SCAN_DOCUMENT);
    assertTrue(scan.contains(
        "|Parameter|Description|Default|\n|---|---|---|\n"
        + "|[`maxRowsQueuedForOrdering`](query-context-reference.md#maxrowsqueuedforordering)|"
        + "The maximum number of rows returned when time ordering is used.|`druid.query.scan.maxRowsQueuedForOrdering`|"
    ));
    assertFalse(scan.contains("`finalize`"));

    final String msq = read(MSQ_DOCUMENT);
    assertTrue(msq.contains("|[`maxNumTasks`](../querying/query-context-reference.md#maxnumtasks)|"));
    assertTrue(msq.endsWith("<!-- END GENERATED QUERY CONTEXT PARAMETER TABLE -->\nafter\n"));

    assertEquals(
        reference,
        Files.readString(
            generatedOutput.resolve(ParameterDocumentationGenerator.REFERENCE_DOCUMENT),
            StandardCharsets.UTF_8
        )
    );
    assertEquals(scan, Files.readString(generatedOutput.resolve(SCAN_DOCUMENT), StandardCharsets.UTF_8));

    ParameterDocumentationGenerator.main(arguments("verify", temporaryFolder.resolve("verified")));
  }

  @Test
  void testVerifyRejectsStaleDocumentation() throws IOException
  {
    writeDocuments(REFERENCE);

    assertGenerationFails("verify", "is stale");
  }

  @Test
  void testVerifyAcceptsCrlfLineEndings() throws IOException
  {
    writeDocuments(REFERENCE);
    writeDocument(SCAN_DOCUMENT, table("queryTypes=SCAN"));
    ParameterDocumentationGenerator.main(arguments("generate", temporaryFolder.resolve("generated")));
    for (final String document : ParameterDocumentationGenerator.DOCUMENTS) {
      writeDocument(document, StringUtils.replace(read(document), "\n", "\r\n"));
    }

    ParameterDocumentationGenerator.main(arguments("verify", temporaryFolder.resolve("verified")));
  }

  @Test
  void testRejectsUnselectedParameters() throws IOException
  {
    writeDocuments(parameters("queryTypes=GROUP_BY"));

    assertGenerationFails("generate", "is not selected by any [PARAMETERS] marker");
  }

  @Test
  void testRejectsParametersSelectedTwice() throws IOException
  {
    writeDocuments(parameters("*") + parameters("queryTypes=GROUP_BY") + parameters("engines~NATIVE"));

    assertGenerationFails("generate", "is selected by more than one filter");
  }

  @Test
  void testRejectsEmptyRemainder() throws IOException
  {
    writeDocuments(parameters("*") + parameters("engines~NATIVE") + parameters("engines!~NATIVE"));

    assertGenerationFails("generate", "filter [*]");
    assertGenerationFails("generate", "selects no parameters");
  }

  @Test
  void testSelectsByNameInReference() throws IOException
  {
    writeDocuments(parameters("*") + "## Vectorization\n" + parameters("names=vectorize,vectorSize"));

    ParameterDocumentationGenerator.main(arguments("generate", temporaryFolder.resolve("generated")));

    final String reference = read(ParameterDocumentationGenerator.REFERENCE_DOCUMENT);
    final int vectorization = reference.indexOf("## Vectorization");
    assertTrue(reference.indexOf("### `vectorize`\n") > vectorization);
    assertTrue(reference.indexOf("### `vectorSize`\n") > vectorization);
    assertTrue(reference.indexOf("### `vectorizeVirtualColumns`\n") < vectorization);
  }

  @Test
  void testTableShowsStatementsOnlyWhenParametersHaveThem() throws IOException
  {
    writeDocuments(REFERENCE);
    writeDocument(SCAN_DOCUMENT, table("queryTypes=SCAN"));
    writeDocument(MSQ_DOCUMENT, table("engines~MSQ engines!~NATIVE"));

    ParameterDocumentationGenerator.main(arguments("generate", temporaryFolder.resolve("generated")));

    assertTrue(read(SCAN_DOCUMENT).contains("|Parameter|Description|Default|\n"));
    final String msq = read(MSQ_DOCUMENT);
    assertTrue(msq.contains("|Parameter|Statements|Description|Default|\n"));
    assertTrue(msq.contains("|[`rowsPerSegment`](../querying/query-context-reference.md#rowspersegment)|INSERT, REPLACE|"));
  }

  @Test
  void testRejectsSeveralRemainders() throws IOException
  {
    writeDocuments(parameters("*") + parameters("*"));

    assertGenerationFails("generate", "may use the filter [*]");
  }

  @Test
  void testRejectsFilterSelectingNothing() throws IOException
  {
    writeDocuments(REFERENCE + parameters("queryTypes=SEARCH engines=DART"));

    assertGenerationFails("generate", "selects no parameters");
  }

  @Test
  void testRejectsParametersOutsideReference() throws IOException
  {
    writeDocuments(REFERENCE);
    writeDocument(SCAN_DOCUMENT, parameters("queryTypes=SCAN"));

    assertGenerationFails("generate", "may contain [PARAMETERS] markers");
  }

  @Test
  void testRejectsEmptyTable() throws IOException
  {
    writeDocuments(REFERENCE);
    writeDocument(SCAN_DOCUMENT, table("queryTypes=SEARCH engines=DART"));

    assertGenerationFails("generate", "selects no parameters");
  }

  @Test
  void testRejectsRemainderTable() throws IOException
  {
    writeDocuments(REFERENCE);
    writeDocument(SCAN_DOCUMENT, table("*"));

    assertGenerationFails("generate", "cannot use the filter [*]");
  }

  @Test
  void testRejectsInvalidFilter() throws IOException
  {
    writeDocuments(REFERENCE);
    writeDocument(SCAN_DOCUMENT, table("queryTypes=UNKNOWN"));

    assertGenerationFails("generate", "Unknown queryTypes value [UNKNOWN]");
  }

  @Test
  void testSelectsByName() throws IOException
  {
    writeDocuments(REFERENCE);
    writeDocument(SCAN_DOCUMENT, table("names=vectorSize,vectorize"));

    ParameterDocumentationGenerator.main(arguments("generate", temporaryFolder.resolve("generated")));

    final String scan = read(SCAN_DOCUMENT);
    assertTrue(scan.indexOf("[`vectorize`]") < scan.indexOf("[`vectorSize`]"));
    assertEquals(2, scan.split("\n\\|\\[`").length - 1);
  }

  @Test
  void testRejectsUnknownName() throws IOException
  {
    writeDocuments(REFERENCE);
    writeDocument(SCAN_DOCUMENT, table("names=vectorize,unknownParameter"));

    assertGenerationFails("generate", "names [unknownParameter], which is not a public query context parameter");
  }

  @Test
  void testRejectsUnterminatedMarker() throws IOException
  {
    writeDocuments(REFERENCE);
    writeDocument(SCAN_DOCUMENT, "<!-- BEGIN GENERATED QUERY CONTEXT PARAMETER TABLE: queryTypes=SCAN -->\n");

    assertGenerationFails("generate", "Missing [<!-- END GENERATED QUERY CONTEXT PARAMETER TABLE -->]");
  }

  @Test
  void testRejectsMissingEndMarkerBeforeNextBlock() throws IOException
  {
    writeDocuments(REFERENCE);
    writeDocument(
        SCAN_DOCUMENT,
        "<!-- BEGIN GENERATED QUERY CONTEXT PARAMETER TABLE: queryTypes=SCAN -->\nprose\n" + table("queryTypes=SEARCH")
    );

    assertGenerationFails("generate", "Missing [<!-- END GENERATED QUERY CONTEXT PARAMETER TABLE -->]");
  }

  @Test
  void testRejectsMismatchedEndMarker() throws IOException
  {
    writeDocuments(REFERENCE);
    writeDocument(
        SCAN_DOCUMENT,
        "<!-- BEGIN GENERATED QUERY CONTEXT PARAMETER TABLE: queryTypes=SCAN -->\n"
        + "<!-- END GENERATED QUERY CONTEXT PARAMETERS -->\n"
    );

    assertGenerationFails("generate", "Missing [<!-- END GENERATED QUERY CONTEXT PARAMETER TABLE -->]");
  }

  @Test
  void testRejectsEndMarkerWithoutBeginMarker() throws IOException
  {
    writeDocuments(REFERENCE);
    writeDocument(SCAN_DOCUMENT, "prose\n<!-- END GENERATED QUERY CONTEXT PARAMETER TABLE -->\n");

    assertGenerationFails("generate", "has no begin marker");
  }

  @Test
  void testTableInReferenceLinksWithinReference() throws IOException
  {
    writeDocuments(REFERENCE + table("queryTypes=SCAN"));

    ParameterDocumentationGenerator.main(arguments("generate", temporaryFolder.resolve("generated")));

    assertTrue(read(ParameterDocumentationGenerator.REFERENCE_DOCUMENT).contains(
        "|[`maxRowsQueuedForOrdering`](query-context-reference.md#maxrowsqueuedforordering)|"
    ));
  }

  @Test
  void testValidateLinks()
  {
    ParameterDocumentationGenerator.validateLinks(
        "p",
        "See [filters](../querying/filters.md#or), [MSQ](../multi-stage-query/reference.md) and [Druid](https://druid.apache.org)."
    );
    for (final String target : new String[]{"#limits", "filters.md", "./filters.md", "http:/druid.apache.org"}) {
      final ISE exception = assertThrows(
          ISE.class,
          () -> ParameterDocumentationGenerator.validateLinks("p", "See [this](" + target + ").")
      );
      assertTrue(exception.getMessage().contains("links to [" + target + "]"), exception.getMessage());
    }
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
  void testSummaryIsFirstSentence()
  {
    assertEquals("First sentence.", ParameterDocumentationGenerator.summary("\nFirst sentence.\nSecond sentence.\n"));
    assertEquals("First sentence.", ParameterDocumentationGenerator.summary("First sentence. Second sentence."));
    assertEquals(
        "Uses `druid.x.y`, e.g. the default.",
        ParameterDocumentationGenerator.summary("Uses `druid.x.y`, e.g. the default. Second sentence.")
    );
    assertEquals("Only sentence.", ParameterDocumentationGenerator.summary("Only sentence."));
    assertEquals("First sentence.", ParameterDocumentationGenerator.summary("First sentence. <br />Second sentence."));
    assertEquals("First part", ParameterDocumentationGenerator.summary("First part<br /><br />second part."));
  }

  @Test
  void testEscapesTableCell()
  {
    assertEquals("a \\| b c<br /><br />d", ParameterDocumentationGenerator.escapeTableCell("a | b\nc\n\nd\n"));
  }

  private static String parameters(final String filter)
  {
    return "<!-- BEGIN GENERATED QUERY CONTEXT PARAMETERS: " + filter + " -->\nstale\n"
           + "<!-- END GENERATED QUERY CONTEXT PARAMETERS -->\n";
  }

  private static String table(final String filter)
  {
    return "<!-- BEGIN GENERATED QUERY CONTEXT PARAMETER TABLE: " + filter + " -->\nstale\n"
           + "<!-- END GENERATED QUERY CONTEXT PARAMETER TABLE -->\n";
  }

  private void assertGenerationFails(final String mode, final String message)
  {
    final ISE exception = assertThrows(
        ISE.class,
        () -> ParameterDocumentationGenerator.main(arguments(mode, temporaryFolder.resolve("generated")))
    );
    assertTrue(exception.getMessage().contains(message), exception.getMessage());
  }

  private String[] arguments(final String mode, final Path generatedOutput)
  {
    return new String[]{temporaryFolder.toString(), mode, generatedOutput.toString()};
  }

  /**
   * Writes the reference document, and an empty file for every other document that may contain generated content.
   */
  private void writeDocuments(final String reference) throws IOException
  {
    for (final String document : ParameterDocumentationGenerator.DOCUMENTS) {
      writeDocument(document, "");
    }
    writeDocument(ParameterDocumentationGenerator.REFERENCE_DOCUMENT, reference);
  }

  private void writeDocument(final String document, final String content) throws IOException
  {
    final Path path = temporaryFolder.resolve(document);
    Files.createDirectories(path.getParent());
    Files.writeString(path, content, StandardCharsets.UTF_8);
  }

  private String read(final String document) throws IOException
  {
    return Files.readString(temporaryFolder.resolve(document), StandardCharsets.UTF_8);
  }
}
