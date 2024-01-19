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

package org.apache.druid.catalog.http;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.CatalogException.DuplicateKeyException;
import org.apache.druid.catalog.CatalogException.NotFoundException;
import org.apache.druid.catalog.model.SchemaRegistry.SchemaSpec;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * REST endpoint for user and internal catalog actions. Catalog actions
 * are divided by operation: configuration-as-code, edits, retrieval,
 * etc. Operations occur at the global level (all schemas), the schema level, or the
 * table level.
 *
 * @see {@link CatalogListenerResource} for the broker-side API.
 */
@Path(CatalogResource.ROOT_PATH)
public class CatalogResource
{
  public static final String ROOT_PATH = "/druid/coordinator/v1/catalog";

  public static final String NAME_FORMAT = "name";
  public static final String PATH_FORMAT = "path";
  public static final String METADATA_FORMAT = "metadata";
  public static final String STATUS_FORMAT = "status";

  private final CatalogStorage catalog;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public CatalogResource(
      final CatalogStorage catalog,
      final AuthorizerMapper authorizerMapper
  )
  {
    this.catalog = catalog;
    this.authorizerMapper = authorizerMapper;
  }

  // ---------------------------------------------------------------------
  // Configuration-as-code style methods

  /**
   * Create or update a new table containing the given table specification.
   * Supports three use cases:
   * <ul>
   * <li>"create": default use case with no options: returns an error if
   *     a table of the same name already exists.</li>
   * <li>"create or update": set {@code overwrite=true}.</li>
   * <li>"update": set {@code version} to the expected current version.
   *     This form enforces optimistic locking.</li>
   * </ul>
   *
   * @param schemaName The name of the Druid schema, which must be writable
   *        and the user must have at least read access.
   * @param tableName The name of the table definition to modify. The user must
   *        have write access to the table.
   * @param spec The new table definition.
   * @param version the expected version of an existing table. The version must
   *        match. If not (or if the table does not exist), returns an error.
   * @param overwrite if {@code true}, then overwrites any existing table.
   *        If {@code false}, then the operation fails if the table already exists.
   *        Ignored if a version is specified.
   * @param req the HTTP request used for authorization.
    */
  @POST
  @Path("/schemas/{schema}/tables/{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response postTable(
      @PathParam("schema") String schemaName,
      @PathParam("name") String tableName,
      TableSpec spec,
      @QueryParam("version") long version,
      @QueryParam("overwrite") boolean overwrite,
      @Context final HttpServletRequest req
  )
  {
    try {
      final SchemaSpec schema = validateSchema(schemaName, true);
      validateTableName(tableName);
      authorizeTable(schema, tableName, Action.WRITE, req);
      validateTableSpec(schema, spec);
      final TableMetadata table = TableMetadata.newTable(TableId.of(schemaName, tableName), spec);
      try {
        catalog.validate(table);
      }
      catch (IAE e) {
        throw CatalogException.badRequest(e.getMessage());
      }

      long newVersion;
      if (version != 0) {
        // A version is provided. Update that version (only).
        newVersion = catalog.tables().update(table, version);
      } else {
        try {
          // No version. Create the table.
          newVersion = catalog.tables().create(table);
        }
        catch (DuplicateKeyException e) {
          // Table exists
          if (overwrite) {
            // User wants to overwrite, so do so.
            newVersion = catalog.tables().replace(table);
          } else {
            throw e;
          }
        }
      }
      return okWithVersion(newVersion);
    }
    catch (CatalogException e) {
      return e.toResponse();
    }
  }

  /**
   * Retrieves the table metadata, including the spec.
   * <p>
   * Returns a 404 (NOT FOUND) error if the table catalog entry does not exist.
   * Note that this check is only for the <i>specification</i>; the table (or
   * datasource) itself may exist. Similarly, this call may return a specification
   * even if there is no datasource of the same name (typically occurs when
   * the definition is created before the datasource itself.)
   *
   * @param schemaName The Druid schema. The user must have read access.
   * @param tableName The name of the table within the schema. The user must have
   *        read access.
   * @param req the HTTP request used for authorization.
   * @return the definition for the table, if any.
   */
  @GET
  @Path("/schemas/{schema}/tables/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTable(
      @PathParam("schema") String schemaName,
      @PathParam("name") String tableName,
      @Context final HttpServletRequest req
  )
  {
    try {
      final SchemaSpec schema = validateSchema(schemaName, false);
      authorizeTable(schema, tableName, Action.READ, req);
      final TableMetadata table = catalog.tables().read(new TableId(schemaName, tableName));
      return Response.ok().entity(table).build();
    }
    catch (CatalogException e) {
      return e.toResponse();
    }
  }

  /**
   * Deletes the table definition (but not the underlying table or datasource)
   * for the given schema and table.
   *
   * @param schemaName The name of the schema that holds the table.
   * @param tableName The name of the table definition to delete. The user must have
   *             write access.
   */
  @DELETE
  @Path("/schemas/{schema}/tables/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteTable(
      @PathParam("schema") String schemaName,
      @PathParam("name") String tableName,
      @Context final HttpServletRequest req
  )
  {
    try {
      final SchemaSpec schema = validateSchema(schemaName, true);
      authorizeTable(schema, tableName, Action.WRITE, req);
      catalog.tables().delete(new TableId(schemaName, tableName));
      return ok();
    }
    catch (CatalogException e) {
      return e.toResponse();
    }
  }

  // ---------------------------------------------------------------------
  // Modify a table within the catalog

  /**
   * Modify an existing table. The edit operations perform incremental changes
   * on a table spec, avoiding the need for the client to download the entire
   * spec to make common changes. The incremental nature avoids the need for
   * optimistic concurrency control using versions: the request applies the
   * change within a transaction using actual locking. The operations are
   * designed so that, in most cases, the results are easy to predict even if
   * the table spec changed between the time it was retrieve and the edit operation
   * is submitted.
   *
   * @param schemaName The name of the schema that holds the table.
   * @param tableName The name of the table definition to delete. The user must have
   *             write access.
   * @param editRequest The operation to perform. See the classes for details.
   */
  @POST
  @Path("/schemas/{schema}/tables/{name}/edit")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response editTable(
      @PathParam("schema") String schemaName,
      @PathParam("name") String tableName,
      TableEditRequest editRequest,
      @Context final HttpServletRequest req
  )
  {
    try {
      final SchemaSpec schema = validateSchema(schemaName, true);
      authorizeTable(schema, tableName, Action.WRITE, req);
      final long newVersion = new TableEditor(catalog, TableId.of(schemaName, tableName), editRequest).go();
      return okWithVersion(newVersion);
    }
    catch (CatalogException e) {
      return e.toResponse();
    }
  }

  // ---------------------------------------------------------------------
  // Retrieval

  /**
   * Retrieves the list of all Druid schema names.
   *
   * @param format the format of the response. See the code for the
   *        available formats
   */
  @GET
  @Path("/schemas")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSchemas(
      @QueryParam("format") String format,
      @Context final HttpServletRequest req
  )
  {
    try {
      format = Strings.isNullOrEmpty(format) ? NAME_FORMAT : StringUtils.toLowerCase(format);
      switch (format) {
        case NAME_FORMAT:
          // No good resource to use: we really need finer-grain control.
          authorizeAccess(ResourceType.STATE, "schemas", Action.READ, req);
          return Response.ok().entity(catalog.schemaRegistry().names()).build();
        case PATH_FORMAT:
          return listTablePaths(req);
        case METADATA_FORMAT:
          return listAllTableMetadata(req);
        default:
          throw CatalogException.badRequest("Unknown format: [%s]", format);
      }
    }
    catch (CatalogException e) {
      return e.toResponse();
    }
  }

  /**
   * Retrieves the list of table names within the given schema for which the
   * user has at least read access. This returns the list of table <i>definitions</i>
   * which will probably differ from the list of actual tables. For example, for
   * the read-only schemas, there will be no table definitions.
   *
   * @param schemaName The name of the Druid schema to query. The user must
   *        have read access.
   * @param format the format of the response. See the code for the
   *        available formats
   */
  @GET
  @Path("/schemas/{schema}/tables")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSchemaTables(
      @PathParam("schema") String schemaName,
      @QueryParam("format") String format,
      @Context final HttpServletRequest req
  )
  {
    try {
      SchemaSpec schema = validateSchema(schemaName, false);
      format = Strings.isNullOrEmpty(format) ? NAME_FORMAT : StringUtils.toLowerCase(format);
      switch (format) {
        case NAME_FORMAT:
          return tableNamesInSchema(schema, req);
        case METADATA_FORMAT:
          return Response.ok().entity(getTableMetadataForSchema(schema, req)).build();
        case STATUS_FORMAT:
          return Response.ok().entity(getTableStatusForSchema(schema, req)).build();
        default:
          throw CatalogException.badRequest("Unknown format: [%s]", format);
      }
    }
    catch (CatalogException e) {
      return e.toResponse();
    }
  }

  // ---------------------------------------------------------------------
  // Sync methods

  public static final String SCHEMA_SYNC = "/sync/schemas/{schema}";

  /**
   * Synchronization request from the Broker for a database schema. Requests all
   * table definitions known to the catalog. Used to prime a cache on first access.
   * After that, the Coordinator will push updates to Brokers. Returns the full
   * list of table details.
   *
   * It is expected that the number of table definitions will be of small or moderate
   * size, so no provision is made to handle very large lists.
   */
  @GET
  @Path(SCHEMA_SYNC)
  @Produces(MediaType.APPLICATION_JSON)
  public Response syncSchema(
      @PathParam("schema") String schemaName,
      @Context final HttpServletRequest req
  )
  {
    // Same as the list schemas endpoint for now. This endpoint reserves the right to change
    // over time as needed, while the user endpoint cannot easily change.
    try {
      SchemaSpec schema = validateSchema(schemaName, false);
      return Response.ok().entity(getTableMetadataForSchema(schema, req)).build();
    }
    catch (CatalogException e) {
      return e.toResponse();
    }
  }

  public static final String TABLE_SYNC = "/sync/schemas/{schema}/{name}";

  /**
   * Synchronization request from the Broker for information about a specific table
   * (datasource). Done on first access to the table by any query. After that, the
   * Coordinator pushes updates to the Broker on any changes.
   */
  @GET
  @Path(TABLE_SYNC)
  @Produces(MediaType.APPLICATION_JSON)
  public Response syncTable(
      @PathParam("schema") String schemaName,
      @PathParam("name") String tableName,
      @Context final HttpServletRequest req
  )
  {
    return getTable(schemaName, tableName, req);
  }

  // ---------------------------------------------------------------------
  // Helper methods

  /**
   * Retrieves the list of all Druid table names for which the user has at
   * least read access.
   */
  private Response listTablePaths(final HttpServletRequest req)
  {
    List<TableId> tables = catalog.tables().allTablePaths();
    Iterable<TableId> filtered = AuthorizationUtils.filterAuthorizedResources(
        req,
        tables,
        tableId -> {
          SchemaSpec schema = catalog.resolveSchema(tableId.schema());
          if (schema == null) {
            // Should never occur.
            return null;
          }
          return Collections.singletonList(
              resourceAction(schema, tableId.name(), Action.READ));
        },
        authorizerMapper
    );
    return Response.ok().entity(Lists.newArrayList(filtered)).build();
  }

  private List<TableMetadata> getTableMetadataForSchema(
      final SchemaSpec schema,
      final HttpServletRequest req
  )
  {
    List<TableMetadata> tables = catalog.tables().tablesInSchema(schema.name());
    Iterable<TableMetadata> filtered = AuthorizationUtils.filterAuthorizedResources(
        req,
        tables,
        table -> {
          TableId tableId = table.id();
          return Collections.singletonList(
              resourceAction(schema, tableId.name(), Action.READ));
        },
        authorizerMapper
    );

    return Lists.newArrayList(filtered);
  }

  private List<TableMetadata> getTableStatusForSchema(
      final SchemaSpec schema,
      final HttpServletRequest req
  )
  {
    // Crude but effective, assuming low volume: get all the data, and throw away
    // the columns and properties.
    return getTableMetadataForSchema(schema, req)
        .stream()
        .map(table -> table.withSpec(new TableSpec(table.spec().type(), null, null)))
        .collect(Collectors.toList());
  }

  private Response listAllTableMetadata(final HttpServletRequest req)
  {
    List<Pair<SchemaSpec, TableMetadata>> tables = new ArrayList<>();
    for (SchemaSpec schema : catalog.schemaRegistry().schemas()) {
      tables.addAll(catalog.tables().tablesInSchema(schema.name())
          .stream()
          .map(table -> Pair.of(schema, table))
          .collect(Collectors.toList()));

    }
    Iterable<Pair<SchemaSpec, TableMetadata>> filtered = AuthorizationUtils.filterAuthorizedResources(
        req,
        tables,
        entry -> {
          return Collections.singletonList(
              resourceAction(entry.lhs, entry.rhs.id().name(), Action.READ));
        },
        authorizerMapper
    );

    List<TableMetadata> metadata = Lists.newArrayList(filtered)
        .stream()
        .map(pair -> pair.rhs)
        .collect(Collectors.toList());
    return Response.ok().entity(metadata).build();
  }

  private Response tableNamesInSchema(
      final SchemaSpec schema,
      final HttpServletRequest req
  )
  {
    List<String> tables = catalog.tables().tableNamesInSchema(schema.name());
    Iterable<String> filtered = AuthorizationUtils.filterAuthorizedResources(
        req,
        tables,
        name ->
          Collections.singletonList(
              resourceAction(schema, name, Action.READ)),
          authorizerMapper
    );
    return Response.ok().entity(Lists.newArrayList(filtered)).build();
  }

  private void validateTableName(String name) throws CatalogException
  {
    try {
      IdUtils.validateId("table", name);
    }
    catch (Exception e) {
      throw CatalogException.badRequest(e.getMessage());
    }
    if (!name.equals(name.trim())) {
      throw CatalogException.badRequest("Table name cannot start or end with spaces");
    }
  }

  private void validateTableSpec(SchemaSpec schema, TableSpec spec) throws CatalogException
  {
    // The given table spec has to be valid for the given schema.
    try {
      spec.validate();
    }
    catch (IAE e) {
      throw CatalogException.badRequest(e.getMessage());
    }

    if (!schema.accepts(spec.type())) {
      throw CatalogException.badRequest(
          "Cannot create tables of type %s in schema %s",
          spec.type(),
          schema.name()
      );
    }
  }

  private SchemaSpec validateSchema(String schemaName, boolean forWrite) throws CatalogException
  {
    if (Strings.isNullOrEmpty(schemaName)) {
      throw CatalogException.badRequest("Schema name is required");
    }
    SchemaSpec schema = catalog.resolveSchema(schemaName);
    if (schema == null) {
      throw new NotFoundException("Unknown schema %s", schemaName);
    }

    if (forWrite && !schema.writable()) {
      throw CatalogException.badRequest(
          "Cannot modify schema %s",
          schemaName
      );
    }
    return schema;
  }

  private static ResourceAction resourceAction(SchemaSpec schema, String tableName, Action action)
  {
    return new ResourceAction(new Resource(tableName, schema.securityResource()), action);
  }

  private void authorizeTable(
      final SchemaSpec schema,
      final String tableName,
      final Action action,
      final HttpServletRequest request
  ) throws CatalogException
  {
    if (Strings.isNullOrEmpty(tableName)) {
      throw CatalogException.badRequest("Table name is required");
    }
    if (action == Action.WRITE && !schema.writable()) {
      throw new ForbiddenException(
          "Cannot create table definitions in schema: " + schema.name());
    }
    authorize(schema.securityResource(), tableName, action, request);
  }

  private void authorize(String resource, String key, Action action, HttpServletRequest request)
  {
    final Access authResult = authorizeAccess(resource, key, action, request);
    if (!authResult.isAllowed()) {
      throw new ForbiddenException(authResult.toString());
    }
  }

  private Access authorizeAccess(String resource, String key, Action action, HttpServletRequest request)
  {
    return AuthorizationUtils.authorizeResourceAction(
        request,
        new ResourceAction(new Resource(key, resource), action),
        authorizerMapper
    );
  }

  private static Response okWithVersion(long version)
  {
    return Response
        .ok()
        .entity(ImmutableMap.of("version", version))
        .build();
  }

  private static Response ok()
  {
    return Response.ok().build();
  }

}
