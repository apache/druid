/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.http;

import com.google.inject.Inject;
import io.druid.db.DatabaseRuleManager;
import io.druid.server.coordinator.rules.Rule;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 */
@Path("/druid/coordinator/v1/rules")
public class RulesResource
{
  private final DatabaseRuleManager databaseRuleManager;

  @Inject
  public RulesResource(
      DatabaseRuleManager databaseRuleManager
  )
  {
    this.databaseRuleManager = databaseRuleManager;
  }

  @GET
  @Produces("application/json")
  public Response getRules()
  {
    return Response.ok(databaseRuleManager.getAllRules()).build();
  }

  @GET
  @Path("/{dataSourceName}")
  @Produces("application/json")
  public Response getDatasourceRules(
      @PathParam("dataSourceName") final String dataSourceName,
      @QueryParam("full") final String full

  )
  {
    if (full != null) {
      return Response.ok(databaseRuleManager.getRulesWithDefault(dataSourceName))
                     .build();
    }
    return Response.ok(databaseRuleManager.getRules(dataSourceName))
                   .build();
  }

  @POST
  @Path("/{dataSourceName}")
  @Consumes("application/json")
  public Response setDatasourceRules(
      @PathParam("dataSourceName") final String dataSourceName,
      final List<Rule> rules
  )
  {
    if (databaseRuleManager.overrideRule(dataSourceName, rules)) {
      return Response.ok().build();
    }
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
  }
}
