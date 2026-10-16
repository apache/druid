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
import org.apache.druid.query.context.DefaultValue;
import org.apache.druid.query.context.QueryContextParameter;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.context.constraint.ParameterConstraint;
import org.apache.druid.query.context.constraint.Range;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Generates the checked-in query context parameter table rows from the descriptor catalog.
 * <p>
 * A row is generated wherever a managed document contains a {@code <!-- GENERATED QUERY CONTEXT PARAMETER: name -->}
 * marker at the end of a line, so a parameter can be documented in more than one table. Every
 * {@link QueryContextParameter.Visibility#PUBLIC} parameter must have at least one marker, and every marker must name
 * a public parameter.
 */
public final class ParameterDocumentationGenerator
{
  /**
   * Managed documents and the row format used in each. Documents whose format is {@link Format#TABLE} or
   * {@link Format#MSQ_TABLE} derive their columns from the header of the table that contains the marker.
   */
  static final Map<String, Format> DOCUMENTS = Map.of(
      "docs/querying/query-context-reference.md", Format.GENERAL,
      "docs/querying/scan-query.md", Format.SCAN,
      "docs/querying/sql-query-context.md", Format.TABLE,
      "docs/querying/groupbyquery.md", Format.TABLE,
      "docs/querying/searchquery.md", Format.TABLE,
      "docs/querying/dart.md", Format.TABLE,
      "docs/multi-stage-query/reference.md", Format.MSQ_TABLE
  );
  private static final Pattern MARKER = Pattern.compile("<!-- GENERATED QUERY CONTEXT PARAMETER: (\\S+) -->$");
  private static final Pattern TABLE_SEPARATOR = Pattern.compile("^\\|\\s*:?-{3,}.*");

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
    final Set<String> unreferenced = new TreeSet<>();
    for (final QueryContextParameter<?> parameter : QueryContextParameters.ALL.get().values()) {
      if (parameter.getVisibility() == QueryContextParameter.Visibility.PUBLIC) {
        unreferenced.add(parameter.getName());
      }
    }

    final Map<Path, String> generatedDocuments = new LinkedHashMap<>();
    for (final String document : new TreeSet<>(DOCUMENTS.keySet())) {
      final Path sourcePath = repositoryRoot.resolve(document);
      final String source = Files.readString(sourcePath, StandardCharsets.UTF_8);
      final String generated = replaceRows(source, DOCUMENTS.get(document), sourcePath, unreferenced);
      final Path generatedPath = generatedOutputDirectory.resolve(document);
      Files.createDirectories(generatedPath.getParent());
      Files.writeString(generatedPath, generated, StandardCharsets.UTF_8);
      generatedDocuments.put(sourcePath, generated);
    }

    if (!unreferenced.isEmpty()) {
      throw new ISE("Missing generated query context parameter markers %s in %s", unreferenced, DOCUMENTS.keySet());
    }

    for (final Map.Entry<Path, String> document : generatedDocuments.entrySet()) {
      final Path sourcePath = document.getKey();
      if (mode == Mode.GENERATE) {
        Files.writeString(sourcePath, document.getValue(), StandardCharsets.UTF_8);
      } else if (!normalizeLineEndings(Files.readString(sourcePath, StandardCharsets.UTF_8))
                  .equals(document.getValue())) {
        throw new ISE(
            "Generated query context documentation is stale in [%s]. "
            + "Run Maven with [-Dquery.context.docs.mode=generate].",
            sourcePath
        );
      }
    }
  }

  private static String replaceRows(
      final String source,
      final Format format,
      final Path sourcePath,
      final Set<String> unreferenced
  )
  {
    final List<String> lines = source.lines().toList();
    final StringBuilder output = new StringBuilder(source.length());
    // Header of the table enclosing the current line, if any. Columns are parsed only for tables with markers.
    String tableHeader = null;

    for (int i = 0; i < lines.size(); i++) {
      final String line = lines.get(i);
      if (!line.startsWith("|")) {
        tableHeader = null;
      } else if (i > 0 && TABLE_SEPARATOR.matcher(line).matches()) {
        tableHeader = lines.get(i - 1);
      }

      final Matcher marker = MARKER.matcher(line);
      if (!marker.find()) {
        output.append(line).append('\n');
        continue;
      }

      final String name = marker.group(1);
      final QueryContextParameter<?> parameter = QueryContextParameters.ALL.get().get(name);
      if (parameter == null || parameter.getVisibility() != QueryContextParameter.Visibility.PUBLIC) {
        throw new ISE("Marker for [%s] in [%s] does not name a public query context parameter", name, sourcePath);
      }
      unreferenced.remove(name);
      final List<Column> columns = format.isHeaderDriven() && tableHeader != null
                                   ? parseColumns(tableHeader, sourcePath)
                                   : null;
      output.append(renderRow(parameter, format, columns, sourcePath)).append(' ').append(marker.group()).append('\n');
    }
    return output.toString();
  }

  private static List<Column> parseColumns(final String header, final Path sourcePath)
  {
    final List<Column> columns = new ArrayList<>();
    for (final String cell : header.substring(1).split("\\|")) {
      final String title = cell.trim().toLowerCase(Locale.ENGLISH);
      if (title.isEmpty()) {
        continue;
      }
      if (columns.isEmpty()) {
        columns.add(Column.NAME);
      } else if ("description".equals(title)) {
        columns.add(Column.DESCRIPTION);
      } else if (title.startsWith("default")) {
        columns.add(Column.DEFAULT);
      } else if ("values".equals(title)) {
        columns.add(Column.VALUES);
      } else {
        throw new ISE("Unsupported query context table column [%s] in [%s]", cell.trim(), sourcePath);
      }
    }
    return columns;
  }

  private static String renderRow(
      final QueryContextParameter<?> parameter,
      final Format format,
      @Nullable final List<Column> columns,
      final Path sourcePath
  )
  {
    final ParameterDocumentation docs = parameter.getDocumentation();
    if (format == Format.SCAN) {
      return StringUtils.format(
          "|%s|%s|%s|%s|",
          parameter.getName(),
          escapeTableCell(docs.getDescription()),
          renderValueDescription(parameter),
          renderDefault(parameter, docs)
      );
    }

    if (format == Format.GENERAL) {
      return StringUtils.format(
          "|`%s`| %s | %s|",
          parameter.getName(),
          renderDefault(parameter, docs),
          escapeTableCell(docs.getDescription())
      );
    }

    if (columns == null) {
      throw new ISE("Marker for [%s] in [%s] is not inside a table", parameter.getName(), sourcePath);
    }
    final StringBuilder row = new StringBuilder("|");
    for (final Column column : columns) {
      switch (column) {
        case NAME:
          row.append('`').append(parameter.getName()).append('`');
          break;
        case DESCRIPTION:
          if (format == Format.MSQ_TABLE && !docs.getStatementTypes().isEmpty()) {
            row.append(docs.getStatementTypes().stream().sorted().map(Enum::name).collect(Collectors.joining(", ")))
               .append("<br /><br />");
          }
          row.append(escapeTableCell(docs.getDescription()));
          break;
        case DEFAULT:
          row.append(renderDefault(parameter, docs));
          break;
        case VALUES:
          row.append(renderValueDescription(parameter));
          break;
        default:
          throw new ISE("Unknown column [%s]", column);
      }
      row.append('|');
    }
    return row.toString();
  }

  private static String renderValueDescription(final QueryContextParameter<?> parameter)
  {
    final StringBuilder description = new StringBuilder();
    description.append(
        switch (parameter.getType()) {
          case INTEGER -> "An integer";
          case LONG -> "A long integer";
          case BOOLEAN -> "A Boolean";
          case STRING -> "A string";
          case ENUM -> parameter.getValueType().getSimpleName();
          default -> parameter.getType().getDisplayName();
        }
    );

    for (final ParameterConstraint<?> constraint : parameter.getConstraints()) {
      if (constraint instanceof Range.Constraint<?> range) {
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
    if (docs.getDefaultDescription().isPresent()) {
      return escapeTableCell(docs.getDefaultDescription().get());
    }
    return switch (parameter.getDefaultValue()) {
      case DefaultValue.Literal<?> literal -> "`" + literal.value() + "`";
      case DefaultValue.FromProperty<?> fromProperty -> "`" + fromProperty.property() + "`";
      case DefaultValue.None<?> _ -> "N/A";
    };
  }

  /**
   * Generated documents always use LF line endings, so a checkout with CRLF line endings is compared after converting
   * them.
   */
  private static String normalizeLineEndings(final String value)
  {
    return StringUtils.replace(value, "\r\n", "\n");
  }

  static String escapeTableCell(final String value)
  {
    return StringUtils.replace(value, "|", "\\|");
  }

  private enum Mode
  {
    VERIFY,
    GENERATE,
    SKIP
  }

  enum Format
  {
    /** The fixed {@code Parameter | Default | Description} layout of query-context-reference.md. */
    GENERAL,
    /** The fixed {@code property | description | values | default} layout of scan-query.md. */
    SCAN,
    /** Columns follow the header of the enclosing table. */
    TABLE,
    /** Like {@link #TABLE}, with the applicable SQL statement types prefixed to the description. */
    MSQ_TABLE;

    boolean isHeaderDriven()
    {
      return this == TABLE || this == MSQ_TABLE;
    }
  }

  private enum Column
  {
    NAME,
    DESCRIPTION,
    DEFAULT,
    VALUES
  }
}
