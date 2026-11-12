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

package org.apache.druid.testing.embedded.lookup;

import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.server.lookup.namespace.NamespaceExtractionModule;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.console.WebConsoleTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Initializes the lookups and adds a JDBC lookup (on a table of the Derby metadata store, as {@link JdbcLookupTest}
 * does) from the Lookups view of the web console, then queries it.
 */
public class JdbcLookupWebConsoleTest extends WebConsoleTestBase
{
  private static final String LOOKUP_TABLE = "web_console_lookups";

  @Override
  protected void configureCluster(EmbeddedDruidCluster cluster)
  {
    cluster.addExtension(NamespaceExtractionModule.class);
    // Push the lookups to the services every second (rather than every 2 minutes), for the Broker to have them soon
    coordinator.addProperty("druid.manager.lookups.period", "1000");
  }

  @BeforeAll
  public void createLookupTable()
  {
    final SQLMetadataConnector connector = coordinator.bindings().sqlMetadataConnector();
    connector.retryWithHandle(
        handle -> handle.update(
            "CREATE TABLE " + LOOKUP_TABLE + "("
            + "  created_date TIMESTAMP NOT NULL,\n"
            + "  country_code VARCHAR(10) NOT NULL,\n"
            + "  country_name VARCHAR(255) NOT NULL,\n"
            + "  PRIMARY KEY (country_code)"
            + ")"
        )
    );
    connector.retryWithHandle(
        handle -> handle.insert(
            "INSERT INTO " + LOOKUP_TABLE
            + " (created_date, country_code, country_name) VALUES"
            + " ('2025-06-01 00:00:00', 'AU', 'Australia'),"
            + " ('2025-06-02 00:00:00', 'PR', 'Puerto Rico')"
        )
    );
  }

  @Test
  public void testJdbcLookup() throws Exception
  {
    runSpec(
        "jdbc-lookup.spec.ts",
        Map.of(
            "DRUID_E2E_TEST_LOOKUP_CONNECT_URI",
            ((TestDerbyConnector) coordinator.bindings().sqlMetadataConnector()).getJdbcUri(),
            "DRUID_E2E_TEST_LOOKUP_TABLE",
            LOOKUP_TABLE
        )
    );
  }
}
