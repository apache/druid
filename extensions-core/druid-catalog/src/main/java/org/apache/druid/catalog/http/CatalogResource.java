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
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.SchemaRegistry.SchemaSpec;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.storage.Actions;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.HideColumns;
import org.apache.druid.catalog.storage.MoveColumn;
import org.apache.druid.catalog.storage.MoveColumn.Position;
import org.apache.druid.catalog.storage.sql.CatalogManager.DuplicateKeyException;
import org.apache.druid.catalog.storage.sql.CatalogManager.NotFoundException;
import org.apache.druid.catalog.storage.sql.CatalogManager.OutOfDateException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceType;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * REST endpoint for user and internal catalog actions. Catalog actions
 * occur at the global level (all schemas), the schema level, or the
 * table level.
 *
 * @see {@link CatalogListenerResource} for the client-side API.
 */
@Path(CatalogResource.ROOT_PATH)
public class CatalogResource
{
  public static final String ROOT_PATH = "/druid/coordinator/v1/catalog";

  private final CatalogStorage catalog;

  @Inject
  public CatalogResource(final CatalogStorage catalog)
  {
    this.catalog = catalog;
  }

  private enum PostAction
  {
    NEW,
    IFNEW,
    REPLACE,
    FORCE;
  }

  /**
   * Create a new table containing the given table specification.
   *
   * @param dbSchema The name of the Druid schema, which must be writable
   *        and the user must have at least read access.
   * @param name The name of the table definition to modify. The user must
   *        have write access to the table.
   * @param spec The new table definition.
   * @param action What to do if the table already exists.
   *        {@code ifNew} is the same as the SQL IF NOT EXISTS clause. If {@code new},
   *        then an error is raised if the table exists. If {@code ifNew}, then
   *        the action silently does nothing if the table exists. Primarily for
   *        use in scripts. The other two options are primarily for use in tests.
   * @param req the HTTP request used for authorization.
   * @return the version number of the table
   */
  @POST
  @Path("/tables/{dbSchema}/{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response postTable(
      @PathParam("dbSchema") String dbSchema,
      @PathParam("name") String name,
      TableSpec spec,
      @QueryParam("action") String actionParam,
      @QueryParam("version") long version,
      @Context final HttpServletRequest req
  )
  {
    final PostAction action;
    if (actionParam == null) {
      action = PostAction.NEW;
    } else {
      action = PostAction.valueOf(StringUtils.toUpperCase(actionParam));
      if (action == null) {
        return Actions.badRequest(
            Actions.INVALID,
            StringUtils.format(
                "Not a valid action: [%s]. Valid actions are new, ifNew, replace, force",
                actionParam
            )
        );
      }
    }
    TableId tableId = TableId.of(dbSchema, name);
    Response response = authorizeTable(tableId, spec, req);
    if (response != null) {
      return response;
    }
    TableMetadata table = TableMetadata.newTable(tableId, spec);
    try {
      catalog.validate(table);
    }
    catch (IAE e) {
      return Actions.badRequest(Actions.INVALID, e.getMessage());
    }

    switch (action) {
      case NEW:
        return insertTableSpec(table, false);
      case IFNEW:
        return insertTableSpec(table, true);
      case REPLACE:
        return updateTableSpec(table, version);
      case FORCE:
        return addOrUpdateTableSpec(table);
      default:
        throw new ISE("Unknown action.");
    }
  }

  private Response authorizeTable(TableId tableId, TableSpec spec, final HttpServletRequest req)
  {
    // Druid has a fixed set of schemas. Ensure the one provided is valid.
    Pair<Response, SchemaSpec> result = validateSchema(tableId.schema());
    if (result.lhs != null) {
      return result.lhs;
    }
    SchemaSpec schema = result.rhs;

    // The schema has to be one that allows table definitions.
    if (!schema.writable()) {
      return Actions.badRequest(
          Actions.INVALID,
          StringUtils.format("Cannot modify schema %s", tableId.schema())
      );
    }

    // Table name can't be blank or have spaces
    if (Strings.isNullOrEmpty(tableId.name())) {
      return Actions.badRequest(Actions.INVALID, "Table name is required");
    }
    if (!tableId.name().equals(tableId.name().trim())) {
      return Actions.badRequest(Actions.INVALID, "Table name cannot start or end with spaces");
    }

    // The given table spec has to be valid for the given schema.
    if (spec != null && !schema.accepts(spec.type())) {
      return Actions.badRequest(
          Actions.INVALID,
          StringUtils.format(
              "Cannot create tables of type %s in schema %s",
              spec == null ? "null" : spec.getClass().getSimpleName(),
                  tableId.schema())
      );
    }

    // The user has to have permission to modify the table.
    try {
      catalog.authorizer().authorizeTable(schema, tableId.name(), Action.WRITE, req);
    }
    catch (ForbiddenException e) {
      return Actions.forbidden(e);
    }

    // Everything checks out, let the request proceed.
    return null;
  }

  private Response insertTableSpec(TableMetadata table, boolean ifNew)
  {
    try {
      long createVersion = catalog.tables().create(table);
      return Actions.okWithVersion(createVersion);
    }
    catch (DuplicateKeyException e) {
      if (!ifNew) {
        return Actions.badRequest(
              Actions.DUPLICATE_ERROR,
              StringUtils.format(
                  "A table of name %s already exists",
                  table.id().sqlName()
              )
        );
      } else {
        return Actions.okWithVersion(0);
      }
    }
    catch (Exception e) {
      return Actions.exception(e);
    }
  }

  private Response updateTableSpec(TableMetadata table, long version)
  {
    try {
      long newVersion = catalog.tables().update(table, version);
      return Actions.okWithVersion(newVersion);
    }
    catch (NotFoundException e) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    catch (OutOfDateException e) {
      return Response
          .status(Response.Status.BAD_REQUEST)
          .entity(
              Actions.error(
                  Actions.DUPLICATE_ERROR,
                  "The table entry not found or is older than the given version: reload and retry"))
          .build();
    }
    catch (Exception e) {
      return Actions.exception(e);
    }
  }

  private Response addOrUpdateTableSpec(TableMetadata table)
  {
    try {
      long newVersion = catalog.tables().create(table);
      return Actions.okWithVersion(newVersion);
    }
    catch (DuplicateKeyException e) {
      // Fall through
    }
    catch (Exception e) {
      return Actions.exception(e);
    }
    try {
      long newVersion = catalog.tables().update(table, 0);
      return Actions.okWithVersion(newVersion);
    }
    catch (Exception e) {
      return Actions.exception(e);
    }
  }

  /**
   * Update a table within the given schema.
   *
   * @param dbSchema The name of the Druid schema, which must be writable
   *        and the user must have at least read access.
   * @param name The name of the table definition to modify. The user must
   *        have write access to the table.
   * @param spec The new table definition.
   * @param version An optional table version. If provided, the metadata DB
   *        entry for the table must be at this exact version or the update
   *        will fail. (Provides "optimistic locking.") If omitted (that is,
   *        if zero), then no update conflict change is done.
   * @param req the HTTP request used for authorization.
   * @return the new version number of the table
   */
  @PUT
  @Path("/tables/{dbSchema}/{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateTableDefn(
      @PathParam("dbSchema") String dbSchema,
      @PathParam("name") String name,
      TableSpec spec,
      @QueryParam("version") long version,
      @Context final HttpServletRequest req
  )
  {

    TableDefnRegistry tableRegistry = catalog.tableRegistry();
    return incrementalUpdate(
        TableId.of(dbSchema, name),
        spec,
        req,
        (existing) -> tableRegistry.resolve(existing).merge(spec).spec()
    );
  }

  private Response incrementalUpdate(
      TableId tableId,
      TableSpec newSpec,
      @Context final HttpServletRequest req,
      Function<TableSpec, TableSpec> action
  )
  {
    Response response = authorizeTable(tableId, newSpec, req);
    if (response != null) {
      return response;
    }
    try {
      long newVersion = catalog.tables().updatePayload(tableId, action);
      return Actions.okWithVersion(newVersion);
    }
    catch (NotFoundException e) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    catch (Exception e) {
      return Actions.exception(e);
    }
  }

  /**
   * Move a single column to the start end of the column list, or before or after
   * another column. Both columns must exist. Returns the version of the table
   * after the update.
   * <p>
   * The operation is done atomically so no optimistic locking is required.
   *
   * @param dbSchema
   * @param name
   * @param command
   * @param req
   * @return
   */
  @POST
  @Path("/tables/{dbSchema}/{name}/moveColumn")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response moveColumn(
      @PathParam("dbSchema") final String dbSchema,
      @PathParam("name") final String name,
      final MoveColumn command,
      @Context final HttpServletRequest req
  )
  {
    if (command == null) {
      return Actions.badRequest(Actions.INVALID, "A MoveColumn object is required");
    }
    if (Strings.isNullOrEmpty(command.column)) {
      return Actions.badRequest(Actions.INVALID, "A column name is required");
    }
    if (command.where == null) {
      return Actions.badRequest(Actions.INVALID, "A target location is required");
    }
    if ((command.where == Position.BEFORE || command.where == Position.AFTER) && Strings.isNullOrEmpty(command.anchor)) {
      return Actions.badRequest(Actions.INVALID, "A anchor column is required for BEFORE or AFTER");
    }
    return incrementalUpdate(
        TableId.of(dbSchema, name),
        null,
        req,
        (spec) -> spec.withColumns(command.perform(spec.columns()))
    );
  }

  /**
   * Hide or unhide columns. If both appear, hide takes precedence. Returns the
   * new table version.
   */
  @POST
  @Path("/tables/{dbSchema}/{name}/hideColumns")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response hideColumns(
      @PathParam("dbSchema") final String dbSchema,
      @PathParam("name") final String name,
      final HideColumns command,
      @Context final HttpServletRequest req
  )
  {
    return incrementalUpdate(
        TableId.of(dbSchema, name),
        null,
        req,
        (spec) -> {
          if (!DatasourceDefn.isDatasource(spec.type())) {
            throw new ISE("hideColumns is supported only for data source specs");
          }
          @SuppressWarnings("unchecked")
          List<String> hiddenProps = (List<String>) spec.properties().get(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY);
          return spec.withProperty(
              DatasourceDefn.HIDDEN_COLUMNS_PROPERTY,
              command.perform(hiddenProps)
          );
        }
    );
  }

  /**
   * Drop column metadata. Only removes metadata entries, has no effect on the
   * physical segments. Returns the new table version.
   */
  @POST
  @Path("/tables/{dbSchema}/{name}/dropColumns")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response dropColumns(
      @PathParam("dbSchema") final String dbSchema,
      @PathParam("name") final String name,
      final List<String> columns,
      @Context final HttpServletRequest req
  )
  {
    return incrementalUpdate(
        TableId.of(dbSchema, name),
        null,
        req,
        (spec) -> spec.withColumns(CatalogUtils.dropColumns(spec.columns(), columns))
    );
  }

  /**
   * Retrieves the definition of the given table.
   * <p>
   * Returns a 404 (NOT FOUND) error if the table definition does not exist.
   * Note that this check is only for the <i>definition</i>; the table (or
   * datasource) itself may exist. Similarly, this call may return a definition
   * even if there is no datasource of the same name (typically occurs when
   * the definition is created before the datasource itself.)
   *
   * @param dbSchema The Druid schema. The user must have read access.
   * @param name The name of the table within the schema. The user must have
   *        read access.
   * @param req the HTTP request used for authorization.
   * @return the definition for the table, if any.
   */
  @GET
  @Path("/tables/{dbSchema}/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTable(
      @PathParam("dbSchema") String dbSchema,
      @PathParam("name") String name,
      @Context final HttpServletRequest req
  )
  {
    Pair<Response, SchemaSpec> result = validateSchema(dbSchema);
    if (result.lhs != null) {
      return result.lhs;
    }
    if (Strings.isNullOrEmpty(name)) {
      return Actions.badRequest(Actions.INVALID, "Table name is required");
    }
    try {
      catalog.authorizer().authorizeTable(result.rhs, name, Action.READ, req);
    }
    catch (ForbiddenException e) {
      return Actions.forbidden(e);
    }
    try {
      TableId tableId = new TableId(dbSchema, name);
      TableMetadata table = catalog.tables().read(tableId);
      if (table == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      return Response.ok().entity(table).build();
    }
    catch (Exception e) {
      return Actions.exception(e);
    }
  }

  /**
   * Retrieves the list of all Druid schema names. At present, Druid does
   * not impose security on schemas, only tables within schemas.
   */
  @GET
  @Path("/list/schemas/names")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listSchemas(
      @Context final HttpServletRequest req
  )
  {
    // No good resource to use: we really need finer-grain control.
    catalog.authorizer().authorizeAccess(ResourceType.STATE, "schemas", Action.READ, req);
    return Response.ok().entity(catalog.schemaRegistry().names()).build();
  }

  /**
   * Retrieves the list of all Druid table names for which the user has at
   * least read access.
   */
  @GET
  @Path("/list/tables/names")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listTables(
      @Context final HttpServletRequest req
  )
  {
    List<TableId> tables = catalog.tables().list();
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
              catalog.authorizer().resourceAction(schema, tableId.name(), Action.READ));
        },
        catalog.authorizer().mapper());
    return Response.ok().entity(Lists.newArrayList(filtered)).build();
  }

  /**
   * Retrieves the list of table names within the given schema for which the
   * user has at least read access. This returns the list of table <i>definitions</i>
   * which will probably differ from the list of actual tables. For example, for
   * the read-only schemas, there will be no table definitions.
   *
   * @param dbSchema The Druid schema to query. The user must have read access.
   */
  @GET
  @Path("/schemas/{dbSchema}/names")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listTables(
      @PathParam("dbSchema") String dbSchema,
      @Context final HttpServletRequest req
  )
  {
    Pair<Response, SchemaSpec> result = validateSchema(dbSchema);
    if (result.lhs != null) {
      return result.lhs;
    }
    SchemaSpec schema = result.rhs;
    List<String> tables = catalog.tables().list(dbSchema);
    Iterable<String> filtered = AuthorizationUtils.filterAuthorizedResources(
        req,
        tables,
        name ->
          Collections.singletonList(
              catalog.authorizer().resourceAction(schema, name, Action.READ)),
        catalog.authorizer().mapper());
    return Response.ok().entity(Lists.newArrayList(filtered)).build();
  }

  /**
   * Retrieves the list of all Druid table metadata for which the user has at
   * least read access.
   */
  @GET
  @Path("/schemas/{dbSchema}/tables")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listTableDetails(
      @PathParam("dbSchema") String dbSchema,
      @Context final HttpServletRequest req
  )
  {
    Pair<Response, SchemaSpec> result = validateSchema(dbSchema);
    if (result.lhs != null) {
      return result.lhs;
    }
    SchemaSpec schema = result.rhs;
    List<TableMetadata> tables = catalog.tables().listDetails(schema.name());
    Iterable<TableMetadata> filtered = AuthorizationUtils.filterAuthorizedResources(
        req,
        tables,
        table -> {
          TableId tableId = table.id();
          return Collections.singletonList(
              catalog.authorizer().resourceAction(schema, tableId.name(), Action.READ));
        },
        catalog.authorizer().mapper());

    return Response.ok().entity(Lists.newArrayList(filtered)).build();
  }

  /**
   * Deletes the table definition (but not the underlying table or datasource)
   * for the given schema and table.
   *
   * @param dbSchema The name of the schema that holds the table.
   * @param name The name of the table definition to delete. The user must have
   *             write access.
   * @param ifExists Optional flag. If {@code false} (the default), 404 (NOT FOUND)
   *                 error is returned if the table does not exist. If {@code true},
   *                 then acts like the SQL IF EXISTS clause and does not return an
   *                 error if the table does not exist,
   */
  @DELETE
  @Path("/tables/{dbSchema}/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteTable(
      @PathParam("dbSchema") String dbSchema,
      @PathParam("name") String name,
      @QueryParam("ifExists") boolean ifExists,
      @Context final HttpServletRequest req
  )
  {
    TableId tableId = new TableId(dbSchema, name);
    Pair<Response, SchemaSpec> result = validateSchema(tableId.schema());
    if (result.lhs != null) {
      return result.lhs;
    }
    SchemaSpec schema = result.rhs;
    if (!schema.writable()) {
      return Actions.badRequest(
          Actions.INVALID,
          StringUtils.format("Cannot delete tables from schema %s", tableId.schema()));
    }
    if (Strings.isNullOrEmpty(name)) {
      return Actions.badRequest(Actions.INVALID, "Table name is required");
    }
    try {
      catalog.authorizer().authorizeTable(schema, tableId.name(), Action.WRITE, req);
    }
    catch (ForbiddenException e) {
      return Actions.forbidden(e);
    }
    try {
      if (!catalog.tables().delete(tableId) && !ifExists) {
        return Actions.notFound(tableId.sqlName());
      }
    }
    catch (Exception e) {
      return Actions.exception(e);
    }
    return Actions.ok();
  }

  public static final String SCHEMA_SYNC = "/schemas/{dbSchema}/sync";

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
      @PathParam("dbSchema") String dbSchema,
      @Context final HttpServletRequest req
  )
  {
    // Same as the user-command for now. This endpoint reserves the right to change
    // over time as needed, while the user endpoint cannot easily change.
    return listTableDetails(dbSchema, req);
  }

  public static final String TABLE_SYNC = "/tables/{dbSchema}/{name}/sync";

  /**
   * Synchronization request from the Broker for information about a specific table
   * (datasource). Done on first access to the table by any query. After that, the
   * Coordinator pushes updates to the Broker on any changes.
   */
  @GET
  @Path(TABLE_SYNC)
  @Produces(MediaType.APPLICATION_JSON)
  public Response syncTable(
      @PathParam("dbSchema") String dbSchema,
      @PathParam("name") String name,
      @Context final HttpServletRequest req
  )
  {
    return getTable(dbSchema, name, req);
  }

  @POST
  @Path("/flush")
  public Response flush(
      @Context final HttpServletRequest req
  )
  {
    // Nothing to do yet.
    return Actions.ok();
  }

  private Pair<Response, SchemaSpec> validateSchema(String dbSchema)
  {
    if (Strings.isNullOrEmpty(dbSchema)) {
      return Pair.of(Actions.badRequest(Actions.INVALID, "Schema name is required"), null);
    }
    SchemaSpec schema = catalog.resolveSchema(dbSchema);
    if (schema == null) {
      return Pair.of(Actions.notFound(
          StringUtils.format("Unknown schema %s", dbSchema)),
          null);
    }
    return Pair.of(null, schema);
  }
}
