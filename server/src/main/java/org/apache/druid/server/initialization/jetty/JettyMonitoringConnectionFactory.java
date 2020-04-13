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

package org.apache.druid.server.initialization.jetty;

import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.util.component.ContainerLifeCycle;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class JettyMonitoringConnectionFactory extends ContainerLifeCycle implements ConnectionFactory
{
  private final ConnectionFactory connectionFactory;
  private final AtomicInteger activeConns;

  public JettyMonitoringConnectionFactory(ConnectionFactory connectionFactory, AtomicInteger activeConns)
  {
    this.connectionFactory = connectionFactory;
    addBean(connectionFactory);
    this.activeConns = activeConns;
  }

  @Override
  public List<String> getProtocols()
  {
    return connectionFactory.getProtocols();
  }

  @Override
  public String getProtocol()
  {
    return connectionFactory.getProtocol();
  }

  @Override
  public Connection newConnection(Connector connector, EndPoint endPoint)
  {
    final Connection connection = connectionFactory.newConnection(connector, endPoint);
    connection.addListener(
        new Connection.Listener()
        {
          @Override
          public void onOpened(Connection connection)
          {
            activeConns.incrementAndGet();
          }

          @Override
          public void onClosed(Connection connection)
          {
            activeConns.decrementAndGet();
          }
        }
    );
    return connection;
  }
}
