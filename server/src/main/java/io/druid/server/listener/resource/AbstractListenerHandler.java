/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.listener.resource;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.ServletResourceUtils;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;

/**
 * This is a simplified handler for announcement listeners. The input is expected to be a JSON list objects.
 *
 * Empty lists `[]` are taken care of at this level and never passed down to the subclass's handle method.
 *
 * @param <ObjType> A List of this type is expected in the input stream as JSON. Must be able to be converted to/from Map<String, Object>
 */
public abstract class AbstractListenerHandler<ObjType> implements ListenerHandler
{
  private static final Logger LOG = new Logger(AbstractListenerHandler.class);
  private final TypeReference<ObjType> inObjTypeRef;

  /**
   * The standard constructor takes in a type reference for the object and for a list of the object.
   * This is to work arround some limitations in Java with type erasure.
   *
   * @param inObjTypeRef The TypeReference for the input object type
   */
  public AbstractListenerHandler(TypeReference<ObjType> inObjTypeRef)
  {
    this.inObjTypeRef = inObjTypeRef;
  }

  @NotNull
  @Override
  public final Response handlePOST(final InputStream inputStream, final ObjectMapper mapper)
  {
    try {
      final Object o = post(ImmutableList.of(mapper.<ObjType>readValue(inputStream, inObjTypeRef)));
      return Response.status(Response.Status.ACCEPTED).entity(o).build();
    }
    catch (JsonParseException | JsonMappingException e) {
      LOG.debug(e, "Bad request");
      return Response.status(Response.Status.BAD_REQUEST).entity(ServletResourceUtils.sanitizeException(e)).build();
    }
    catch (Exception e) {
      LOG.error(e, "Error handling request");
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @NotNull
  @Override
  public final Response handlePOSTAll(final InputStream inputStream, final ObjectMapper mapper)
  {
    final List<ObjType> inObjList;
    try {
      // This actually fails to properly convert due to type erasure. We'll try again in a second
      // This effectively just parses
      final List<Object> tempList = mapper.readValue(inputStream, new TypeReference<List<Object>>()
      {
      });

      // Now do the ACTUAL conversion
      inObjList = ImmutableList.copyOf(Lists.transform(tempList, new Function<Object, ObjType>()
      {
        @Override
        public ObjType apply(Object input)
        {
          return mapper.convertValue(input, inObjTypeRef);
        }
      }));
    }
    catch (final IOException ex) {
      LOG.debug(ex, "Bad request");
      return Response.status(Response.Status.BAD_REQUEST).entity(ServletResourceUtils.sanitizeException(ex)).build();
    }
    final Object returnObj;
    try {
      returnObj = post(inObjList);
    }
    catch (Exception e) {
      LOG.error(e, "Error handling request");
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
    if (returnObj == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    } else {
      return Response.status(Response.Status.ACCEPTED).entity(returnObj).build();
    }
  }

  @Override
  @NotNull
  public final Response handleGET(@NotNull String id)
  {
    try {
      final Object returnObj = get(id);
      if (returnObj == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      } else {
        return Response.ok(returnObj).build();
      }
    }
    catch (Exception e) {
      LOG.error(e, "Error handling get request for [%s]", id);
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @Override
  @NotNull
  public final Response handleGETAll()
  {
    final Collection<ObjType> all;
    try {
      all = getAll();
      if (all == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      } else {
        return Response.ok(all).build();
      }
    }
    catch (Exception e) {
      LOG.error(e, "Error getting all");
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @Override
  @NotNull
  public final Response handleDELETE(@NotNull String id)
  {
    try {
      final Object returnObj = delete(id);
      if (returnObj == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      } else {
        return Response.status(Response.Status.ACCEPTED).entity(returnObj).build();
      }
    }
    catch (Exception e) {
      LOG.error(e, "Error in processing delete request for [%s]", id);
      return Response.serverError().entity(ServletResourceUtils.sanitizeException(e)).build();
    }
  }

  @Override
  public final void use_AbstractListenerHandler_instead()
  {
    // NOOP
  }

  /**
   * Delete the object for a particular id
   *
   * @param id A string id of the object to be deleted. This id is never null or empty.
   *
   * @return The object to be returned in the entity. A NULL return will cause a 404 response. A non-null return will cause a 202 response. An Exception thrown will cause a 500 response.
   */
  protected abstract
  @Nullable
  Object delete(@NotNull String id);

  /**
   * Get the object for a particular id
   *
   * @param id A string id of the object desired. This id is never null or empty.
   *
   * @return The object to be returned in the entity. A NULL return will cause a 404 response. A non-null return will cause a 200 response. An Exception thrown will cause a 500 response.
   */
  protected abstract
  @Nullable
  Object get(@NotNull String id);

  protected abstract
  @Nullable
  Collection<ObjType> getAll();

  /**
   * Process a POST request of the input items
   *
   * @param inputObject A list of the objects which were POSTed
   *
   * @return An object to be returned in the entity of the response.
   *
   * @throws Exception
   */
  public abstract
  @Nullable
  Object post(@NotNull List<ObjType> inputObject) throws Exception;
}
