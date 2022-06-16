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

package org.apache.druid.server.http;

import com.google.common.base.Strings;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.druid.catalog.Actions;
import org.apache.druid.catalog.CatalogStorage;
import org.apache.druid.catalog.SchemaRegistry.SchemaDefn;
import org.apache.druid.catalog.TableId;
import org.apache.druid.catalog.TableMetadata;
import org.apache.druid.catalog.TableSpec;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.catalog.CatalogManager;
import org.apache.druid.metadata.catalog.CatalogManager.DuplicateKeyException;
import org.apache.druid.metadata.catalog.CatalogManager.NotFoundException;
import org.apache.druid.metadata.catalog.CatalogManager.OutOfDateException;
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
  public CatalogResource(CatalogStorage catalog)
  {
    this.catalog = catalog;
  }

  /**
   * Create a new table within the indicated schema.
   *
   * @param table The table definition to create.
   * @param ifNew Whether to skip the action if the table already exists.
   *        This is the same as the SQL IF NOT EXISTS clause. If {@code false},
   *        then an error is raised if the table exists. If {@code true}, then
   *        the action silently does nothing if the table exists. Primarily for
   *        use in scripts.
   * @param req the HTTP request used for authorization.
   * @return the version number of the table
   */
  @POST
  @Path("/tables")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response createTable(
      TableMetadata table,
      @QueryParam("ifnew") boolean ifNew,
      @Context final HttpServletRequest req)
  {
    String dbSchema = table.resolveDbSchema();
    Pair<Response, SchemaDefn> result = validateSchema(dbSchema);
    if (result.lhs != null) {
      return result.lhs;
    }
    SchemaDefn schema = result.rhs;
    if (!schema.writable()) {
      return Actions.badRequest(
          Actions.INVALID,
          StringUtils.format("Cannot create tables in schema %s", dbSchema));
    }
    table = table.withSchema(dbSchema);
    try {
      table.validate();
    }
    catch (IAE e) {
      return Actions.badRequest(Actions.INVALID, e.getMessage());
    }
    TableSpec defn = table.defn();
    if (!schema.accepts(defn)) {
      return Actions.badRequest(
          Actions.INVALID,
          StringUtils.format(
              "Cannot create tables of type %s in schema %s",
              defn == null ? "null" : defn.getClass().getSimpleName(),
              dbSchema));
    }
    try {
      catalog.authorizer().authorizeTable(schema, table.name(), Action.WRITE, req);
    }
    catch (ForbiddenException e) {
      return Actions.forbidden(e);
    }
    try {
      long createVersion = catalog.tables().create(table);
      return Actions.okWithVersion(createVersion);
    }
    catch (DuplicateKeyException e) {
      if (!ifNew) {
        return Actions.badRequest(
              Actions.DUPLICATE_ERROR,
              StringUtils.format(
                  "A table of name %s.%s aleady exists",
                  table.dbSchema(),
                  table.name()));
      } else {
        return Actions.okWithVersion(0);
      }
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
   * @param defn The new table definition.
   * @param version An optional table version. If provided, the metadata DB
   *        entry for the table must be at this exact version or the update
   *        will fail. (Provides "optimistic locking.") If omitted (that is,
   *        if zero), then no update conflict change is done.
   * @param req the HTTP request used for authorization.
   * @return the new version number of the table
   */
  @POST
  @Path("/tables/{dbSchema}/{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateTableDefn(
      @PathParam("dbSchema") String dbSchema,
      @PathParam("name") String name,
      TableSpec defn,
      @QueryParam("version") long version,
      @Context final HttpServletRequest req)
  {
    try {
      if (defn != null) {
        defn.validate();
      }
    }
    catch (IAE e) {
      return Actions.badRequest(Actions.INVALID, e.getMessage());
    }
    Pair<Response, SchemaDefn> result = validateSchema(dbSchema);
    if (result.lhs != null) {
      return result.lhs;
    }
    if (Strings.isNullOrEmpty(name)) {
      return Actions.badRequest(Actions.INVALID, "Table name is required");
    }
    SchemaDefn schema = result.rhs;
    if (!schema.writable()) {
      return Actions.badRequest(
          Actions.INVALID,
          StringUtils.format("Cannot update tables in schema %s", dbSchema));
    }
    if (!schema.accepts(defn)) {
      return Actions.badRequest(
          Actions.INVALID,
          StringUtils.format(
              "Cannot update tables to type %s in schema %s",
              defn == null ? "null" : defn.getClass().getSimpleName(),
              dbSchema));
    }
    try {
      catalog.authorizer().authorizeTable(schema, name, Action.WRITE, req);
    }
    catch (ForbiddenException e) {
      return Actions.forbidden(e);
    }
    try {
      CatalogManager tableMgr = catalog.tables();
      TableId tableId = new TableId(dbSchema, name);
      long newVersion;
      if (version == 0) {
        newVersion = tableMgr.updateDefn(tableId, defn);
      } else {
        newVersion = tableMgr.updateDefn(tableId, defn, version);
      }
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
      @Context final HttpServletRequest req)
  {
    Pair<Response, SchemaDefn> result = validateSchema(dbSchema);
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
  @Path("/schemas")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listSchemas(
      @Context final HttpServletRequest req)
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
  @Path("/tables")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listTables(
      @Context final HttpServletRequest req)
  {
    List<TableId> tables = catalog.tables().list();
    Iterable<TableId> filtered = AuthorizationUtils.filterAuthorizedResources(
        req,
        tables,
        tableId -> {
          SchemaDefn schema = catalog.resolveSchema(tableId.schema());
          if (schema == null) {
            // Should never occur.
            return null;
          }
          return Collections.singletonList(
              catalog.authorizer().resourceAction(schema, tableId.name(), Action.READ));
        },
        catalog.authorizer().mapper());
    List<TableId> filteredList = new ArrayList<>();
    for (TableId tableId : filtered) {
      filteredList.add(tableId);
    }
    return Response.ok().entity(filteredList).build();
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
  @Path("/tables/{dbSchema}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listTables(
      @PathParam("dbSchema") String dbSchema,
      @Context final HttpServletRequest req)
  {
    Pair<Response, SchemaDefn> result = validateSchema(dbSchema);
    if (result.lhs != null) {
      return result.lhs;
    }
    SchemaDefn schema = result.rhs;
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
      @Context final HttpServletRequest req)
  {
    TableId tableId = new TableId(dbSchema, name);
    Pair<Response, SchemaDefn> result = validateSchema(tableId.schema());
    if (result.lhs != null) {
      return result.lhs;
    }
    SchemaDefn schema = result.rhs;
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

  public static final String SCHEMA_SYNC = "/sync/{dbSchema}";

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
    Pair<Response, SchemaDefn> result = validateSchema(dbSchema);
    if (result.lhs != null) {
      return result.lhs;
    }
    SchemaDefn schema = result.rhs;
    List<TableMetadata> tables = catalog.tables().listDetails(dbSchema);
    Iterable<TableMetadata> filtered = AuthorizationUtils.filterAuthorizedResources(
        req,
        tables,
        table ->
          Collections.singletonList(
              catalog.authorizer().resourceAction(schema, table.name(), Action.READ)),
        catalog.authorizer().mapper());
    return Response.ok().entity(Lists.newArrayList(filtered)).build();
  }

  public static final String TABLE_SYNC = "/sync/{dbSchema}/{name}";

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
      @Context final HttpServletRequest req)
  {
    return getTable(dbSchema, name, req);
  }

  private Pair<Response, SchemaDefn> validateSchema(String dbSchema)
  {
    if (Strings.isNullOrEmpty(dbSchema)) {
      return Pair.of(Actions.badRequest(Actions.INVALID, "Schema name is required"), null);
    }
    SchemaDefn schema = catalog.resolveSchema(dbSchema);
    if (schema == null) {
      return Pair.of(Actions.notFound(
          StringUtils.format("Unknown schema %s", dbSchema)),
          null);
    }
    return Pair.of(null, schema);
  }
}
