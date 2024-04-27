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

package org.apache.druid.catalog.model.table;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.ModelProperties.ObjectPropertyDefn;
import org.apache.druid.catalog.model.ModelProperties.PropertyDefn;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefn;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;

import java.util.Arrays;
import java.util.Map;

/**
 * Definition of an external table, primarily for ingestion.
 * The components are derived from those for Druid ingestion: an
 * input source, a format and a set of columns. Also provides
 * properties, as do all table definitions.
 *
 * <h4>Partial Tables and Connections</h4>
 *
 * An input source is a template for an external table. The input
 * source says how to get data, and optionally the format and structure of that data.
 * Since Druid never ingests the same data twice, the actual external table needs
 * details that says which data to read on any specific ingestion. Thus, an external
 * table is usually a "partial table": all the information that remains constant
 * across ingestions, but without the information that changes. The changing
 * information is typically the list of files (or objects or URLs) to ingest.
 * <p>
 * The pattern is:<br>
 * {@code external table spec + parameters --> external table}
 * <p>
 * Since an input source is a parameterized (partial) external table, we can reuse
 * the table metadata structures and APIs, avoiding the need to have a separate (but
 * otherwise identical) structure for external tables.
 *
 * An external table can be thought of as a "connection", though Druid does not
 * use that term. When used as a connection, the external table spec will omit the
 * format. Instead, the format will also be provided at ingest time, along with the
 * list of tables (or objects.)
 * <p>
 * To keep all this straight, we adopt the following terms:
 * <dl>
 * <dt>External table spec</dt>
 * <dd>The JSON serialized version of an external table which can be partial or
 * complete. The spec is a named entry in the Druid catalog</dd>
 * <dt>Complete spec</dt>
 * <dd>An external table spec that provides all information needed to access an
 * external table. Each use identifies the same set of data. Useful if MSQ is used
 * to query an external data source. A complete spec can be referenced as a
 * first-class table in a {@code FROM} clause in an MSQ query.</dd>
 * <dt>Partial spec</dt>
 * <dd>An external table spec that omits some information. That information must
 * be provided at query time in the form of a {@code TABLE} function. If the partial
 * spec includes a format, then it is essentially a <i>partial table</i>. If it
 * omits the format, then it is essentially a <i>connection</i>.</dd>
 * <dt>Completed table</dt>
 * <dd>The full external table that results from a partial spec and a set of SQL
 * table function parameters.</dd>
 * <dt>Ad-hoc table</dt>
 * <dd>Users can define an external table using the generic {@code EXTERN} function
 * or one of the input-source-specific functions. In this case, there is no
 * catalog entry: all information comes from the SQL table function</dd>
 * <dt>Partial table function</dt>
 * <dd>The SQL table function used to "complete" a partial spec. The function
 * defines parameters to fill in the missing information. The function is generated
 * on demand and has the same name as the catalog entry for the partial spec.
 * The function will include parameters for format if the catalog spec does not
 * specify a format. Else, the format parameters are omitted and the completed
 * table uses the format provided in the catalog spec.</dd>
 * <dt>Ad-hoc table function</dt>
 * <dd>The SQL table function used to create an ad-hoc external table. The function
 * as a name defined by the {@link InputFormatDefn}, and has parameters for all
 * support formats: the user must specify all input source and format properties.</dd>
 * </dl>
 *
 * <h4>External Table Structure</h4>
 *
 * The external table is generic: it represents all valid combinations of input
 * sources and formats. Rather than have a different table definition for each, we
 * instead split out input sources and formats into their own definitions, and those
 * definitions are integrated and used by this external table class. As a result,
 * the {@code properties} field will contain the {@code source} property which has
 * the JSON serialized form of the input source (minus items to be parameterized.)
 * <p>
 * Similarly, if the external table also defines a format (rather than requiring the
 * format at ingest time), then the {@code format} property holds the JSON-serialized
 * form of the input format, minus columns. The columns can be provided in the spec,
 * in the {@code columns} field. The {@link InputFormatDefn} converts the columns to
 * the form needed by the input format.
 * <p>
 * Druid's input sources all require formats. However, some sources may not actually
 * need the format. A JDBC input source for example, needs no format. In other cases,
 * there may be a subset of formats. Each {@link InputSourceDefn} is responsible for
 * working out which formats (if any) are required. This class is agnostic about whether
 * the format is supplied. (Remember that, when used as a connection, the external table
 * will provide no format until ingest time.)
 * <p>
 * By contrast, the input source is always required.
 *
 * <h4>Data Formats and Conversions</h4>
 *
 * Much of the code here handles conversion of an external table specification to
 * the form needed by SQL. Since SQL is not visible here, we instead create an
 * instance of {@link ExternalTableSpec} which holds the input source, input format
 * and row signature in the form required by SQL.
 * <p>
 * This class handles table specifications in three forms:
 * <ol>
 * <li>From a fully-defined table specification, converted to a {@code ExternalTableSpec}
 * by the {@link #convert(ResolvedTable)} function.</li>
 * <li>From a fully-defined set of arguments to a SQL table function. The
 * {@link InputSourceDefn#adHocTableFn()} method provides the function definition which
 * handles the conversion.</li>
 * <li>From a partially-defined table specification in the catalog, augmented by
 * parameters passed from a SQL function. The {@link #tableFn(ResolvedTable)} method
 * creates the required function by caching the table spec. That function then combines
 * the parameters to produce the required {@code ExternalTableSpec}.</li>
 * </ol>
 * <p>
 * To handle these formats, and the need to adjust JSON, conversion to an
 * {@code ExternalTableSpec} occurs in multiple steps:
 * <ul>
 * <li>When using a table spec, the serialized JSON is first converted to a generic
 * Java map: one for the input source, another for the format.</li>
 * <li>When using a SQL function, the SQL arguments are converted (if needed) and
 * written into a Java map. If the function references an existing table spec: then
 * the JSON map is first populated with the deserialized spec.</li>
 * <li>Validation and/or adjustments are made to the Java map. Adjustments are
 * those described elsewhere in this Javadoc.</li>
 * <li>The column specifications from either SQL or the table spec are converted to
 * a list of column names, and placed into the Java map for the input format.</li>
 * <li>The maps are converted to the {@link InputSource} or {@link InputFormat}
 * objects using a Jackson conversion.</li>
 * </ul>
 * The actual conversions are handled in the {@link InputFormatDefn} and
 * {@link InputFormatDefn} classes, either directly (for a fully-defined table
 * function) or starting here (for other use cases).
 *
 * <h4>Property and Parameter Names</h4>
 *
 * Pay careful attention to names: the names may be different in each of the above
 * cases:
 * <ul>
 * <li>The table specification stores the input source and input format specs using
 * the names defined by the classes themselves. That is, the table spec holds a string
 * that represents the Jackson-serialized form of those classes. In some cases, the
 * JSON can be a subset: some sources and formats have obscure checks, or options which
 * are not available via this path. The code that does conversions will adjust the JSON
 * prior to conversion. Each JSON object has a type field: the value of that type
 * field must match that defined in the Jackson annotations for the corresponding
 * class.</li>
 * <li>SQL table functions use argument names that are typically selected for user
 * convenience, and may not be the same as the JSON field name. For example, a field
 * name may be a SQL reserved word, or may be overly long, or may be obscure. The code
 * for each input source and input format definition does the needed conversion.</li>
 * <li>Each input source and input format has a type. The input format type is given,
 * in SQL by the {@code format} property. The format type name is typically the same
 * as the JSON type name, but need not be.</li>
 * </ul>
 *
 * <h4>Extensions</h4>
 *
 * This class is designed to work both with "well known" Druid input sources and formats,
 * and those defined in an extension. For extension-defined sources and formats to work,
 * the extension must define an {@link InputSourceDefn} or {@link InputFormatDefn} which
 * are put into the {@link TableDefnRegistry} and thus available to this class. The result
 * is that this class is ignorant of the actual details of sources and formats: it instead
 * delegates to the input source and input format definitions for that work.
 * <p>
 * Input sources and input formats defined in an extension are considered "ephemeral":
 * they can go away if the corresponding extension is removed from the system. In that
 * case, any table functions defined by those extensions are no longer available, and
 * any SQL statements that use those functions will no longer work. The catalog may contain
 * an external table spec that references those definitions. Such specs will continue to
 * reside in the catalog, and can be retrieved, but they will fail any query that attempts
 * to reference them.
 */
public class ExternalTableDefn extends TableDefn
{
  /**
   * Identifier for external tables.
   */
  public static final String TABLE_TYPE = "extern";

  /**
   * Column type for external tables.
   */
  public static final String EXTERNAL_COLUMN_TYPE = "extern";

  /**
   * Property which holds the input source specification as serialized as JSON.
   */
  public static final String SOURCE_PROPERTY = "source";

  /**
   * Property which holds the optional input format specification, serialized as JSON.
   */
  public static final String FORMAT_PROPERTY = "format";

  /**
   * Definition of the input source property.
   */
  private static final PropertyDefn<InputSource> SOURCE_PROPERTY_DEFN =
      new ObjectPropertyDefn<InputSource>(SOURCE_PROPERTY, InputSource.class);

  /**
   * Definition of the input format property.
   */
  private static final PropertyDefn<InputFormat> FORMAT_PROPERTY_DEFN =
      new ObjectPropertyDefn<InputFormat>(FORMAT_PROPERTY, InputFormat.class);

  /**
   * Type reference used to deserialize JSON to a generic map.
   */
  @VisibleForTesting
  public static final TypeReference<Map<String, Object>> MAP_TYPE_REF = new TypeReference<Map<String, Object>>() { };

  private TableDefnRegistry registry;

  public ExternalTableDefn()
  {
    super(
        "External table",
        TABLE_TYPE,
        Arrays.asList(
            SOURCE_PROPERTY_DEFN,
            FORMAT_PROPERTY_DEFN
        ),
        null
    );
  }

  @Override
  public void bind(TableDefnRegistry registry)
  {
    this.registry = registry;
  }

  @Override
  public void validate(ResolvedTable table)
  {
    for (PropertyDefn<?> propDefn : properties().values()) {
      // Validate everything except the input source and input format: those
      // are done elsewhere since they are complex and require context.
      if (propDefn != SOURCE_PROPERTY_DEFN && propDefn != FORMAT_PROPERTY_DEFN) {
        propDefn.validate(table.property(propDefn.name()), table.jsonMapper());
      }
    }
    validateColumns(table.spec().columns());
    new ResolvedExternalTable(table).validate(registry);
  }

  /**
   * Return a table function definition for a partial table as given by
   * the catalog table spec. The function defines parameters to gather the
   * values needed to convert the partial table into a fully-defined table
   * which can be converted to an {@link ExternalTableSpec}.
   */
  public TableFunction tableFn(ResolvedTable table)
  {
    return new ResolvedExternalTable(table).resolve(registry).tableFn();
  }

  @Override
  protected void validateColumn(ColumnSpec colSpec)
  {
    // Validate type in next PR
  }

  /**
   * Return the {@link ExternalTableSpec} for a catalog entry for a
   * fully-defined table. This form exists for completeness, since ingestion never
   * reads the same data twice. This form is handy for tests, and will become
   * generally useful when MSQ fully supports queries and those queries can
   * read from external tables.
   */
  public ExternalTableSpec convert(ResolvedTable table)
  {
    return new ResolvedExternalTable(table).resolve(registry).convert();
  }

  public static boolean isExternalTable(ResolvedTable table)
  {
    return TABLE_TYPE.equals(table.spec().type());
  }
}
