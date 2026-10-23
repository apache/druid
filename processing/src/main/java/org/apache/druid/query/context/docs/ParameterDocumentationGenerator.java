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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.context.DefaultValue;
import org.apache.druid.query.context.QueryContextParameter;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.context.constraint.ParameterConstraint;
import org.apache.druid.query.context.constraint.Range;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.IdentityHashMap;
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
 * Generates the checked-in query context parameter documentation from the descriptor catalog.
 * <p>
 * Documents choose what to generate, and where, with pairs of markers whose {@link ParameterFilter} selects the
 * parameters to render. The generator only renders parameters; headings and the text around the markers belong to the
 * documents. There are two kinds of markers:
 * <ul>
 *   <li>{@code <!-- BEGIN GENERATED QUERY CONTEXT PARAMETERS: filter -->} ...
 *   {@code <!-- END GENERATED QUERY CONTEXT PARAMETERS -->} renders each selected parameter in full, as a heading
 *   followed by a table of its description and properties. These markers may only appear in
 *   {@link #REFERENCE_DOCUMENT}, where every {@link QueryContextParameter.Visibility#PUBLIC} parameter must be selected
 *   by exactly one of them.</li>
 *   <li>{@code <!-- BEGIN GENERATED QUERY CONTEXT PARAMETER TABLE: filter -->} ...
 *   {@code <!-- END GENERATED QUERY CONTEXT PARAMETER TABLE -->} renders a table that summarizes the selected
 *   parameters and links each of them to its entry in {@link #REFERENCE_DOCUMENT}.</li>
 * </ul>
 * Parameters are sorted by name within each marker.
 */
public final class ParameterDocumentationGenerator
{
  static final String REFERENCE_DOCUMENT = "docs/querying/query-context-reference.md";

  /**
   * Documents that may contain generated content, including {@link #REFERENCE_DOCUMENT}.
   */
  static final Set<String> DOCUMENTS = Set.of(
      REFERENCE_DOCUMENT,
      "docs/querying/scan-query.md",
      "docs/querying/sql-query-context.md",
      "docs/querying/groupbyquery.md",
      "docs/querying/searchquery.md",
      "docs/querying/dart.md",
      "docs/querying/vectorization.md",
      "docs/multi-stage-query/reference.md"
  );

  private static final Pattern BEGIN_MARKER =
      Pattern.compile("^<!-- BEGIN GENERATED QUERY CONTEXT (PARAMETERS|PARAMETER TABLE): (.+) -->$");
  private static final Pattern END_MARKER =
      Pattern.compile("^<!-- END GENERATED QUERY CONTEXT (PARAMETERS|PARAMETER TABLE) -->$");
  /**
   * Targets of Markdown links. Descriptions are rendered in documents in different directories, so their links must
   * work from any of them: absolute URLs, or paths relative to a directory under {@code docs/}, such as
   * {@code ../querying/filters.md}.
   */
  private static final Pattern LINK_TARGET = Pattern.compile("\\]\\(([^)\\s]*)\\)");
  private static final Pattern SENTENCE_END = Pattern.compile("\\.\\s+(?=[A-Z<])|\\s*<br\\s*/?>");
  private static final Comparator<QueryContextParameter<?>> BY_NAME =
      Comparator.comparing((QueryContextParameter<?> parameter) -> parameter.getName(), String.CASE_INSENSITIVE_ORDER)
                .thenComparing(QueryContextParameter::getName);

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
    final List<QueryContextParameter<?>> parameters = publicParameters();
    final Path referencePath = repositoryRoot.resolve(REFERENCE_DOCUMENT);

    final Map<String, String> generatedDocuments = new LinkedHashMap<>();
    for (final String document : new TreeSet<>(DOCUMENTS)) {
      final Path sourcePath = repositoryRoot.resolve(document);
      final String referenceLink = toLink(sourcePath.getParent().relativize(referencePath));
      generatedDocuments.put(
          document,
          generate(read(sourcePath), parameters, REFERENCE_DOCUMENT.equals(document), referenceLink, sourcePath)
      );
    }

    for (final Map.Entry<String, String> document : generatedDocuments.entrySet()) {
      final Path generatedPath = generatedOutputDirectory.resolve(document.getKey());
      Files.createDirectories(generatedPath.getParent());
      Files.writeString(generatedPath, document.getValue(), StandardCharsets.UTF_8);
    }

    for (final Map.Entry<String, String> document : generatedDocuments.entrySet()) {
      final Path sourcePath = repositoryRoot.resolve(document.getKey());
      if (mode == Mode.GENERATE) {
        Files.writeString(sourcePath, document.getValue(), StandardCharsets.UTF_8);
      } else if (!read(sourcePath).equals(document.getValue())) {
        throw new ISE(
            "Generated query context documentation is stale in [%s]. "
            + "Run Maven with [-Dquery.context.docs.mode=generate].",
            sourcePath
        );
      }
    }
  }

  private static List<QueryContextParameter<?>> publicParameters()
  {
    final List<QueryContextParameter<?>> parameters =
        QueryContextParameters.ALL.get()
                                  .values()
                                  .stream()
                                  .filter(parameter -> parameter.getVisibility()
                                                       == QueryContextParameter.Visibility.PUBLIC)
                                  .sorted(BY_NAME)
                                  .collect(Collectors.toList());

    final Set<String> anchors = new TreeSet<>();
    for (final QueryContextParameter<?> parameter : parameters) {
      if (!anchors.add(anchor(parameter))) {
        throw new ISE("Query context parameter [%s] has the same anchor as another parameter", parameter.getName());
      }
      validateLinks(parameter.getName(), parameter.getDocumentation().getDescription());
      // Default descriptions are rendered wherever descriptions are, so their links must work from any directory too.
      parameter.getDocumentation()
               .getDefaultDescription()
               .ifPresent(defaultDescription -> validateLinks(parameter.getName(), defaultDescription));
    }
    return parameters;
  }

  /**
   * Checks that every link in a description works from any directory under {@code docs/}, since descriptions are
   * rendered in documents in several directories.
   */
  static void validateLinks(final String name, final String description)
  {
    final Matcher matcher = LINK_TARGET.matcher(description);
    while (matcher.find()) {
      final String target = matcher.group(1);
      if (!target.startsWith("../") && !target.startsWith("http://") && !target.startsWith("https://")) {
        throw new ISE(
            "Description of query context parameter [%s] links to [%s], which only works from some documents. "
            + "Link to a path relative to a directory under docs/, such as [../querying/filters.md], "
            + "or to an absolute URL.",
            name,
            target
        );
      }
    }
  }

  /**
   * Returns the source with the content between every pair of markers regenerated.
   */
  static String generate(
      final String source,
      final List<QueryContextParameter<?>> parameters,
      final boolean isReference,
      final String referenceLink,
      final Path sourcePath
  )
  {
    final List<String> lines = source.lines().toList();
    final Set<String> parameterNames =
        parameters.stream().map(QueryContextParameter::getName).collect(Collectors.toSet());
    final List<Block> blocks = parseBlocks(lines, parameterNames, sourcePath);
    final List<Block> fullBlocks = blocks.stream().filter(block -> block.kind == Kind.PARAMETERS).toList();
    if (!isReference && !fullBlocks.isEmpty()) {
      throw new ISE(
          "Only [%s] may contain [%s] markers, but [%s] does",
          REFERENCE_DOCUMENT,
          Kind.PARAMETERS.markerName,
          sourcePath
      );
    }
    final Map<Block, List<QueryContextParameter<?>>> assignment =
        isReference ? assign(fullBlocks, parameters, sourcePath) : new IdentityHashMap<>();

    final StringBuilder output = new StringBuilder(source.length());
    int next = 0;
    for (final Block block : blocks) {
      for (int i = next; i <= block.beginLine; i++) {
        output.append(lines.get(i)).append('\n');
      }
      output.append('\n');
      if (block.kind == Kind.PARAMETERS) {
        for (final QueryContextParameter<?> parameter : assignment.get(block)) {
          output.append(renderParameter(parameter)).append('\n');
        }
      } else {
        output.append(renderTable(block, parameters, referenceLink, sourcePath)).append('\n');
      }
      next = block.endLine;
    }
    for (int i = next; i < lines.size(); i++) {
      output.append(lines.get(i)).append('\n');
    }
    return output.toString();
  }

  private static List<Block> parseBlocks(
      final List<String> lines,
      final Set<String> parameterNames,
      final Path sourcePath
  )
  {
    final List<Block> blocks = new ArrayList<>();
    for (int i = 0; i < lines.size(); i++) {
      if (END_MARKER.matcher(lines.get(i)).matches()) {
        throw new ISE("[%s] in [%s] has no begin marker", lines.get(i), sourcePath);
      }
      final Matcher begin = BEGIN_MARKER.matcher(lines.get(i));
      if (!begin.matches()) {
        continue;
      }
      final Kind kind = Kind.of(begin.group(1));
      final ParameterFilter filter;
      try {
        filter = ParameterFilter.parse(begin.group(2));
      }
      catch (IAE e) {
        throw new ISE(e, "%s in [%s]", e.getMessage(), sourcePath);
      }
      for (final String name : filter.getNames()) {
        if (!parameterNames.contains(name)) {
          throw new ISE(
              "Query context parameter filter [%s] in [%s] names [%s], which is not a public query context parameter",
              filter,
              sourcePath,
              name
          );
        }
      }

      // Stop at any other marker, so that a missing end marker fails instead of swallowing the text up to the next
      // block's end marker.
      int end = i + 1;
      while (end < lines.size()
             && !BEGIN_MARKER.matcher(lines.get(end)).matches()
             && !END_MARKER.matcher(lines.get(end)).matches()) {
        end++;
      }
      if (end == lines.size() || !kind.endMarker().equals(lines.get(end))) {
        throw new ISE("Missing [%s] after [%s] in [%s]", kind.endMarker(), lines.get(i), sourcePath);
      }
      blocks.add(new Block(kind, filter, i, end));
      i = end;
    }
    return blocks;
  }

  /**
   * Assigns every parameter to the one {@link Kind#PARAMETERS} block whose filter selects it, or to the block with the
   * {@link ParameterFilter#isRemainder() remainder} filter if no other block selects it.
   */
  private static Map<Block, List<QueryContextParameter<?>>> assign(
      final List<Block> blocks,
      final List<QueryContextParameter<?>> parameters,
      final Path sourcePath
  )
  {
    final List<Block> remainders = blocks.stream().filter(block -> block.filter.isRemainder()).toList();
    if (remainders.size() > 1) {
      throw new ISE("At most one [%s] marker in [%s] may use the filter [*]", Kind.PARAMETERS.markerName, sourcePath);
    }

    final Map<Block, List<QueryContextParameter<?>>> assignment = new IdentityHashMap<>();
    for (final Block block : blocks) {
      assignment.put(block, new ArrayList<>());
    }
    for (final QueryContextParameter<?> parameter : parameters) {
      final List<Block> matches =
          blocks.stream().filter(block -> block.filter.matches(parameter.getName(), parameter.getDocumentation())).toList();
      if (matches.size() > 1) {
        throw new ISE(
            "Query context parameter [%s] is selected by more than one filter in [%s]: %s",
            parameter.getName(),
            sourcePath,
            matches.stream().map(block -> "[" + block.filter + "]").collect(Collectors.joining(", "))
        );
      }
      if (matches.isEmpty() && remainders.isEmpty()) {
        throw new ISE(
            "Query context parameter [%s] is not selected by any [%s] marker in [%s]",
            parameter.getName(),
            Kind.PARAMETERS.markerName,
            sourcePath
        );
      }
      assignment.get(matches.isEmpty() ? remainders.get(0) : matches.get(0)).add(parameter);
    }
    for (final Map.Entry<Block, List<QueryContextParameter<?>>> entry : assignment.entrySet()) {
      if (entry.getValue().isEmpty()) {
        throw new ISE(
            "Query context parameter filter [%s] in [%s] selects no parameters",
            entry.getKey().filter,
            sourcePath
        );
      }
    }
    return assignment;
  }

  private static String renderTable(
      final Block block,
      final List<QueryContextParameter<?>> parameters,
      final String referenceLink,
      final Path sourcePath
  )
  {
    if (block.filter.isRemainder()) {
      throw new ISE("[%s] markers in [%s] cannot use the filter [*]", Kind.TABLE.markerName, sourcePath);
    }
    final List<QueryContextParameter<?>> selected =
        parameters.stream().filter(parameter -> block.filter.matches(parameter.getName(), parameter.getDocumentation())).toList();
    if (selected.isEmpty()) {
      throw new ISE("Query context parameter filter [%s] in [%s] selects no parameters", block.filter, sourcePath);
    }

    // Only tables of parameters that apply to specific SQL statements, such as those of SQL-based ingestion, need to
    // show which statements each parameter applies to.
    final boolean showStatements =
        selected.stream().anyMatch(parameter -> !parameter.getDocumentation().getStatementTypes().isEmpty());
    final StringBuilder output = new StringBuilder(
        showStatements
        ? "|Parameter|Statements|Description|Default|\n|---|---|---|---|\n"
        : "|Parameter|Description|Default|\n|---|---|---|\n"
    );
    for (final QueryContextParameter<?> parameter : selected) {
      final ParameterDocumentation docs = parameter.getDocumentation();
      output.append(StringUtils.format("|[`%s`](%s#%s)|", parameter.getName(), referenceLink, anchor(parameter)));
      if (showStatements) {
        // Left empty for parameters whose metadata doesn't restrict them to particular statements.
        output.append(renderStatementTypes(docs)).append('|');
      }
      output.append(escapeTableCell(summary(docs.getDescription())))
            .append('|')
            .append(escapeTableCell(renderDefault(parameter, docs)))
            .append("|\n");
    }
    return output.toString();
  }

  /**
   * Renders a parameter as a heading, which puts it in the page outline, followed by a two-column table of its
   * description and properties, which visually separates it from its neighbors.
   */
  private static String renderParameter(final QueryContextParameter<?> parameter)
  {
    final ParameterDocumentation docs = parameter.getDocumentation();
    final StringBuilder output = new StringBuilder();
    output.append("### `").append(parameter.getName()).append("`\n\n")
          .append("| | |\n|---|---|\n");
    appendRow(output, "Description", docs.getDescription());
    appendRow(output, "Type", renderType(parameter));
    appendRow(output, "Default", renderDefault(parameter, docs));
    if (!docs.getQueries().isEmpty()) {
      appendRow(output, "Query", docs.getQueries().stream().sorted().map(Enum::name).collect(Collectors.joining(", ")));
    }
    if (!docs.getEngines().isEmpty()) {
      appendRow(
          output,
          "Engine",
          docs.getEngines().stream().sorted().map(ParameterDocumentationGenerator::engineName).collect(Collectors.joining(", "))
      );
    }
    if (!docs.getQueryTypes().isEmpty()) {
      appendRow(
          output,
          "Query type",
          docs.getQueryTypes().stream().sorted().map(Object::toString).collect(Collectors.joining(", "))
      );
    }
    if (!docs.getStatementTypes().isEmpty()) {
      appendRow(
          output,
          "Statement",
          renderStatementTypes(docs)
      );
    }
    if (parameter.getSince() != null) {
      appendRow(output, "Since", parameter.getSince());
    }
    return output.toString();
  }

  private static void appendRow(final StringBuilder output, final String name, final String value)
  {
    output.append("|**").append(name).append("**|").append(escapeTableCell(value)).append("|\n");
  }

  /**
   * Escapes a value for a single table cell: a table cell cannot contain a line break, so paragraph breaks become
   * {@code <br />} tags and other line breaks become spaces.
   */
  static String escapeTableCell(final String value)
  {
    return StringUtils.replace(
        StringUtils.replace(StringUtils.replace(value.strip(), "|", "\\|"), "\n\n", "<br /><br />"),
        "\n",
        " "
    );
  }

  /**
   * Returns the anchor of a parameter's heading in {@link #REFERENCE_DOCUMENT}: Docusaurus derives it from the heading
   * text by lowercasing it. Explicit heading IDs, which could keep the parameter's case, are avoided because the docs
   * spellchecker flags camel-case words in them.
   */
  static String anchor(final QueryContextParameter<?> parameter)
  {
    return StringUtils.toLowerCase(parameter.getName());
  }

  private static String renderStatementTypes(final ParameterDocumentation docs)
  {
    return docs.getStatementTypes().stream().sorted().map(Enum::name).collect(Collectors.joining(", "));
  }

  private static String engineName(final ParameterDocumentation.Engine engine)
  {
    return switch (engine) {
      case NATIVE -> "Native";
      case MSQ -> "MSQ";
      case DART -> "Dart";
    };
  }

  private static String renderType(final QueryContextParameter<?> parameter)
  {
    final StringBuilder description = new StringBuilder(
        parameter.getType() == QueryContextParameter.Type.HUMAN_READABLE_NUMBER
        ? "Number, or a [human-readable](../configuration/human-readable-byte.md) value such as `10K` or `256MiB`"
        : parameter.getType().getDisplayName()
    );
    if (parameter.getValueType().isEnum()) {
      // The string form of a parameter's enum constant is its JSON form, as QueryContextParameterTest verifies.
      description.append(" (")
                 .append(
                     Arrays.stream(parameter.getValueType().getEnumConstants())
                           .map(constant -> "`" + constant + "`")
                           .collect(Collectors.joining(", "))
                 )
                 .append(')');
    }

    // Other constraints are custom validation logic, which the parameter's description explains.
    for (final ParameterConstraint<?> constraint : parameter.getConstraints()) {
      if (constraint instanceof Range.Constraint<?> range) {
        description.append(" in [")
                   .append(range.getLowerBound())
                   .append(", ")
                   .append(range.getUpperBound())
                   .append(']');
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
      return docs.getDefaultDescription().get();
    }
    return switch (parameter.getDefaultValue()) {
      case DefaultValue.Literal<?> literal -> "`" + literal.value() + "`";
      case DefaultValue.FromProperty<?> fromProperty -> "`" + fromProperty.property() + "`";
      case DefaultValue.None<?> _ -> "N/A";
    };
  }

  /**
   * Returns the first sentence of a description: the text up to the first period that is followed by whitespace and an
   * uppercase letter or a tag, or up to the first line break tag, whichever comes first. Returns the whole description
   * if there is neither.
   */
  static String summary(final String description)
  {
    final String stripped = description.strip();
    final Matcher matcher = SENTENCE_END.matcher(stripped);
    if (!matcher.find()) {
      return stripped;
    }
    return stripped.charAt(matcher.start()) == '.'
           ? stripped.substring(0, matcher.start() + 1)
           : stripped.substring(0, matcher.start());
  }

  private static String toLink(final Path relativePath)
  {
    return StringUtils.replace(relativePath.toString(), "\\", "/");
  }

  /**
   * Generated documents always use LF line endings, so a checkout with CRLF line endings is compared after converting
   * them.
   */
  private static String read(final Path path) throws IOException
  {
    return StringUtils.replace(Files.readString(path, StandardCharsets.UTF_8), "\r\n", "\n");
  }

  private enum Mode
  {
    VERIFY,
    GENERATE,
    SKIP
  }

  private enum Kind
  {
    PARAMETERS("PARAMETERS"),
    TABLE("PARAMETER TABLE");

    private final String markerName;

    Kind(final String markerName)
    {
      this.markerName = markerName;
    }

    static Kind of(final String markerName)
    {
      return PARAMETERS.markerName.equals(markerName) ? PARAMETERS : TABLE;
    }

    String endMarker()
    {
      return "<!-- END GENERATED QUERY CONTEXT " + markerName + " -->";
    }
  }

  /**
   * A pair of markers, at the given line indexes, and the filter of its begin marker.
   */
  private record Block(Kind kind, ParameterFilter filter, int beginLine, int endLine)
  {
  }
}
