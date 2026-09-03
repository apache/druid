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
import org.apache.druid.query.context.constraint.ParameterConstraint;
import org.apache.druid.query.context.constraint.Range;
import org.apache.druid.query.context.docs.ParameterDocumentation.Query;
import org.apache.druid.query.context.docs.ParameterDocumentation.QueryType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Generates the checked-in query context parameter table rows from the descriptor catalog.
 */
public final class ParameterDocumentationGenerator
{
  private static final String GENERAL_REFERENCE = "docs/querying/query-context-reference.md";
  private static final String SCAN_REFERENCE = "docs/querying/scan-query.md";
  private static final String SQL_REFERENCE = "docs/querying/sql-query-context.md";
  private static final String MARKER_FORMAT = "<!-- GENERATED QUERY CONTEXT PARAMETER: %s -->";

  private ParameterDocumentationGenerator()
  {
  }

  public static void main(final String[] args) throws IOException
  {
    if (args.length != 3) {
      throw new ISE("Expected arguments: <repository root> <verify|generate|skip> <generated output directory>");
    }

    final Mode mode = Mode.valueOf(args[1].toUpperCase(Locale.ENGLISH));
    if (mode == Mode.SKIP) {
      return;
    }

    final Path repositoryRoot = Path.of(args[0]);
    final Path generatedOutputDirectory = Path.of(args[2]);
    final Map<String, Map<String, String>> rowsByDocument = renderRows();

    for (final Map.Entry<String, Map<String, String>> document : rowsByDocument.entrySet()) {
      final Path sourcePath = repositoryRoot.resolve(document.getKey());
      final String source = Files.readString(sourcePath, StandardCharsets.UTF_8);
      final String generated = replaceRows(source, document.getValue(), sourcePath);
      final Path generatedPath = generatedOutputDirectory.resolve(document.getKey());
      Files.createDirectories(generatedPath.getParent());
      Files.writeString(generatedPath, generated, StandardCharsets.UTF_8);

      if (mode == Mode.GENERATE) {
        Files.writeString(sourcePath, generated, StandardCharsets.UTF_8);
      } else if (!source.equals(generated)) {
        throw new ISE(
            "Generated query context documentation is stale in [%s]. "
            + "Run Maven with [-Dquery.context.docs.mode=generate].",
            sourcePath
        );
      }
    }
  }

  private static Map<String, Map<String, String>> renderRows()
  {
    final Map<String, Map<String, String>> rowsByDocument = new LinkedHashMap<>();
    for (final QueryContextParameter<?> parameter : QueryContextParameters.BY_NAME.values()) {
      final ParameterDocumentation docs = parameter.getDocumentation().orElse(null);
      if (docs == null) {
        continue;
      }
      final String document;
      if (docs.getQueries().contains(Query.SQL) && !docs.getQueries().contains(Query.JSON)) {
        document = SQL_REFERENCE;
      } else if (docs.getQueryTypes().contains(QueryType.SCAN)) {
        document = SCAN_REFERENCE;
      } else {
        document = GENERAL_REFERENCE;
      }
      rowsByDocument.computeIfAbsent(document, ignored -> new LinkedHashMap<>())
                    .put(parameter.getName(), renderRow(parameter, docs, document));
    }
    return rowsByDocument;
  }

  private static String renderRow(
      final QueryContextParameter<?> parameter,
      final ParameterDocumentation docs,
      final String document
  )
  {
    if (SCAN_REFERENCE.equals(document)) {
      return StringUtils.format(
          "|%s|%s|%s|%s|",
          parameter.getName(),
          escapeTableCell(docs.getDescription()),
          renderValueDescription(parameter),
          renderDefault(parameter, docs)
      );
    }

    if (SQL_REFERENCE.equals(document)) {
      return StringUtils.format(
          "|`%s`|%s|%s|",
          parameter.getName(),
          escapeTableCell(docs.getDescription()),
          renderDefault(parameter, docs)
      );
    }

    return StringUtils.format(
        "|`%s`| %s | %s|",
        parameter.getName(),
        renderDefault(parameter, docs),
        escapeTableCell(docs.getDescription())
    );
  }

  private static String renderValueDescription(final QueryContextParameter<?> parameter)
  {
    final StringBuilder description = new StringBuilder();
    if (Integer.class.equals(parameter.getValueType())) {
      description.append("An integer");
    } else if (Long.class.equals(parameter.getValueType())) {
      description.append("A long integer");
    } else if (Boolean.class.equals(parameter.getValueType())) {
      description.append("A Boolean");
    } else if (String.class.equals(parameter.getValueType())) {
      description.append("A string");
    } else {
      description.append(parameter.getValueType().getSimpleName());
    }

    for (final ParameterConstraint<?> constraint : parameter.getConstraints()) {
      if (constraint instanceof Range.Constraint<?>) {
        final Range.Constraint<?> range = (Range.Constraint<?>) constraint;
        description.append(" in [")
                   .append(range.getLowerBound())
                   .append(", ")
                   .append(range.getUpperBound())
                   .append(']');
      } else {
        throw new ISE(
            "Documentation generation is not implemented for constraint [%s]",
            constraint.getClass().getName()
        );
      }
    }
    return description.toString();
  }

  private static String renderDefault(
      final QueryContextParameter<?> parameter,
      final ParameterDocumentation docs
  )
  {
    return docs.getDefaultDescription()
               .map(ParameterDocumentationGenerator::normalizeTableCell)
               .or(() -> parameter.getDefaultValue().map(value -> "`" + value + "`"))
               .orElse("N/A");
  }

  private static String replaceRows(
      final String source,
      final Map<String, String> rows,
      final Path sourcePath
  )
  {
    final List<String> lines = source.lines().toList();
    final StringBuilder output = new StringBuilder(source.length());
    final Map<String, String> remainingRows = new LinkedHashMap<>(rows);

    for (final String line : lines) {
      boolean replaced = false;
      for (final Map.Entry<String, String> row : List.copyOf(remainingRows.entrySet())) {
        final String marker = StringUtils.format(MARKER_FORMAT, row.getKey());
        if (line.endsWith(marker)) {
          output.append(row.getValue()).append(' ').append(marker).append('\n');
          remainingRows.remove(row.getKey());
          replaced = true;
          break;
        }
      }
      if (!replaced) {
        output.append(line).append('\n');
      }
    }

    if (!remainingRows.isEmpty()) {
      throw new ISE("Missing generated query context parameter markers %s in [%s]", remainingRows.keySet(), sourcePath);
    }
    return output.toString();
  }

  private static String escapeTableCell(final String value)
  {
    return StringUtils.replace(normalizeTableCell(value), "|", "\\|");
  }

  /**
   * Markdown table cells must be rendered on one physical line. Parameter descriptions and default descriptions are
   * commonly written as Java text blocks, so remove text-block line continuations, flatten remaining line breaks, and
   * collapse incidental whitespace before writing the generated documentation.
   */
  static String normalizeTableCell(final String value)
  {
    return value.replaceAll("\\\\[ \\t]*\\R", "")
                .replaceAll("\\R", " ")
                .replaceAll("[ \\t]+", " ")
                .trim();
  }

  private enum Mode
  {
    VERIFY,
    GENERATE,
    SKIP
  }
}
