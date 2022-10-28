package org.apache.druid.catalog.http;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.CatalogException.DuplicateKeyException;
import org.apache.druid.catalog.model.SchemaRegistry.SchemaSpec;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.storage.Actions;
import org.apache.druid.catalog.storage.CatalogStorage;
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

/**
 * REST endpoint for user and internal catalog actions. Catalog actions
 * are divided by operation: configuration-as-code, edits, retrieval,
 * etc. Operations occur at the global level (all schemas), the schema level, or the
 * table level.
 *
 * @see {@link CatalogListenerResource} for the broker-side API.
 */
public class CatalogResource2
{
  public static final String ROOT_PATH = "/druid/coordinator/v1/catalog";

  private final CatalogStorage catalog;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public CatalogResource2(
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
   * <li>"create if not exists": default use case with no options.</li>
   * <li>"create or update": set {@code overwrite=true}.</li>
   * <li>"update": set {@code version} to the expected current version.
   *     This form enforces optimistic locking.</li>
   * </ul>
   *
   * @param dbSchema The name of the Druid schema, which must be writable
   *        and the user must have at least read access.
   * @param name The name of the table definition to modify. The user must
   *        have write access to the table.
   * @param spec The new table definition.
   * @param version the expected version of an existing table. The version must
   *        match. If not (or if the table does not exist), returns an error.
   * @param overwrite if {@code true}, then overwrites any existing table.
   *        If {@code false}, then the operation fails if the table already exists.
   * @param req the HTTP request used for authorization.
    */
  @POST
  @Path("/resource/tables/{dbSchema}/{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response postTable(
      @PathParam("dbSchema") String dbSchema,
      @PathParam("name") String name,
      TableSpec spec,
      @QueryParam("version") long version,
      @QueryParam("overwrite") boolean overwrite,
      @Context final HttpServletRequest req
  )
  {
    try {
      final SchemaSpec schema = validateSchema(dbSchema, true);
      validateTable(schema, name, spec, req);
      final TableMetadata table = TableMetadata.newTable(TableId.of(dbSchema, name), spec);
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
    catch (CatalogException e)
    {
      return e.toResponse();
    }
  }

  /**
   * Retrieves a table spec.
   * <p>
   * Returns a 404 (NOT FOUND) error if the table definition does not exist.
   * Note that this check is only for the <i>specification</i>; the table (or
   * datasource) itself may exist. Similarly, this call may return a specification
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
  @Path("/resource/tables/{dbSchema}/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTable(
      @PathParam("dbSchema") String dbSchema,
      @PathParam("name") String name,
      @Context final HttpServletRequest req
  )
  {
    try {
      final SchemaSpec schema = validateSchema(dbSchema, false);
      authorizeTable(schema, name, Action.READ, req);
      final TableMetadata table = catalog.tables().read(new TableId(dbSchema, name));
      return Response.ok().entity(table.spec()).build();
    }
    catch (CatalogException e)
    {
      return e.toResponse();
    }
  }

  /**
   * Deletes the table definition (but not the underlying table or datasource)
   * for the given schema and table.
   *
   * @param dbSchema The name of the schema that holds the table.
   * @param name The name of the table definition to delete. The user must have
   *             write access.
   */
  @DELETE
  @Path("/tables/{dbSchema}/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteTable(
      @PathParam("dbSchema") String dbSchema,
      @PathParam("name") String name,
      @Context final HttpServletRequest req
  )
  {
    try {
      final SchemaSpec schema = validateSchema(dbSchema, true);
      authorizeTable(schema, name, Action.WRITE, req);
      catalog.tables().delete(new TableId(dbSchema, name));
      return ok();
    }
    catch (CatalogException e)
    {
      return e.toResponse();
    }
  }

  // ---------------------------------------------------------------------
  // Helper methods

  private void validateTable(SchemaSpec schema, String name, TableSpec spec, final HttpServletRequest req) throws CatalogException
  {
    // Table name can't be blank or have leading/trailing spaces
    if (Strings.isNullOrEmpty(name)) {
      throw CatalogException.badRequest("Table name is required");
    }
    if (!name.equals(name.trim())) {
      throw CatalogException.badRequest("Table name cannot start or end with spaces");
    }

    // The user has to have permission to modify the table.
    authorizeTable(schema, name, Action.WRITE, req);

    // Validate the spec, if provided.
    if (spec != null) {

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

    // Everything checks out, let the request proceed.
  }

  private SchemaSpec validateSchema(String dbSchema, boolean forWrite) throws CatalogException
  {
    if (Strings.isNullOrEmpty(dbSchema)) {
      throw CatalogException.badRequest("Schema name is required");
    }
    SchemaSpec schema = catalog.resolveSchema(dbSchema);
    if (schema == null) {
      throw CatalogException.badRequest("Unknown schema %s", dbSchema);
    }

    if (forWrite && !schema.writable()) {
      throw CatalogException.badRequest(
          "Cannot modify schema %s",
          dbSchema
      );
    }
    return schema;
  }

  private void authorizeTable(SchemaSpec schema, String name, Action action, HttpServletRequest request) throws CatalogException
  {
    if (Strings.isNullOrEmpty(name)) {
      throw CatalogException.badRequest("Table name is required");
    }
    if (action == Action.WRITE && !schema.writable()) {
      throw new ForbiddenException(
          "Cannot create table definitions in schema: " + schema.name());
    }
    authorize(schema.securityResource(), name, action, request);
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
