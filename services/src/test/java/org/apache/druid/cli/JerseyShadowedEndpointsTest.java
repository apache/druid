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

package org.apache.druid.cli;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.util.List;

public class JerseyShadowedEndpointsTest
{
  @Test
  public void testEndpointUnderMoreSpecificPathOfAnotherResourceIsShadowed()
  {
    Assertions.assertEquals(
        List.of(
            "[POST /druid/indexer/v1/datasources/{dataSource}/shutdownAllTasks]"
            + " of [" + IndexerResource.class.getName() + ".shutdownAllTasks()]"
            + " is shadowed by [" + DataSourcesResource.class.getName() + "]"
            + " with @Path[/druid/indexer/v1/datasources]"
        ),
        JerseyShadowedEndpoints.find(List.of(IndexerResource.class, DataSourcesResource.class))
    );
  }

  @Test
  public void testEndpointInMostSpecificResourceIsNotShadowed()
  {
    Assertions.assertEquals(
        List.of(),
        JerseyShadowedEndpoints.find(
            List.of(IndexerResourceWithoutShutdown.class, DataSourcesResourceWithShutdown.class, StatusResource.class)
        )
    );
  }

  @Test
  public void testSubResourceLocatorUnderMoreSpecificPathOfAnotherResourceIsShadowed()
  {
    Assertions.assertEquals(
        List.of(
            "[ANY /druid/indexer/v1/datasources/{dataSource}/handler]"
            + " of [" + IndexerResourceWithLocator.class.getName() + ".getHandler()]"
            + " is shadowed by [" + DataSourcesResource.class.getName() + "]"
            + " with @Path[/druid/indexer/v1/datasources]"
        ),
        JerseyShadowedEndpoints.find(List.of(IndexerResourceWithLocator.class, DataSourcesResource.class))
    );
  }

  @Test
  public void testClassWithoutPathIsIgnored()
  {
    Assertions.assertEquals(
        List.of(),
        JerseyShadowedEndpoints.find(List.of(NotAResource.class, StatusResource.class))
    );
  }

  /**
   * Same layout as {@code OverlordResource} and {@code OverlordDataSourcesResource} had before the
   * {@code shutdownAllTasks} endpoint moved.
   */
  @Path("/druid/indexer/v1")
  public static class IndexerResource
  {
    @GET
    @Path("/task/{taskId}")
    public void getTask()
    {
    }

    @POST
    @Path("/datasources/{dataSource}/shutdownAllTasks")
    public void shutdownAllTasks()
    {
    }
  }

  @Path("/druid/indexer/v1/datasources")
  public static class DataSourcesResource
  {
    @DELETE
    @Path("/{dataSource}")
    public void markAllSegmentsAsUnused()
    {
    }
  }

  @Path("/druid/indexer/v1")
  public static class IndexerResourceWithoutShutdown
  {
    @GET
    @Path("/task/{taskId}")
    public void getTask()
    {
    }
  }

  @Path("/druid/indexer/v1/datasources")
  public static class DataSourcesResourceWithShutdown
  {
    @DELETE
    @Path("/{dataSource}")
    public void markAllSegmentsAsUnused()
    {
    }

    @POST
    @Path("/{dataSource}/shutdownAllTasks")
    public void shutdownAllTasks()
    {
    }
  }

  @Path("/druid/indexer/v1")
  public static class IndexerResourceWithLocator
  {
    @Path("/datasources/{dataSource}/handler")
    public Object getHandler()
    {
      return new Object();
    }
  }

  @Path("/status")
  public static class StatusResource
  {
    @GET
    public void getStatus()
    {
    }
  }

  public static class NotAResource
  {
    @GET
    public void get()
    {
    }
  }
}
