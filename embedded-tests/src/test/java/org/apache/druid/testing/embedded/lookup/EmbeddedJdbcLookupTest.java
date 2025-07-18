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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.server.lookup.namespace.NamespaceExtractionModule;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Embedded test to verify JDBC lookups.
 */
public class EmbeddedJdbcLookupTest extends EmbeddedClusterTestBase
{
  private static final String JDBC_LOOKUP_TABLE = "embedded_lookups";
  private static final String BULK_UPDATE_LOOKUP_PAYLOAD
      = "{"
        + "  \"__default\": {"
        + "    \"%s\": {"
        + "      \"version\": \"v1\","
        + "      \"lookupExtractorFactory\": {"
        + "        \"type\": \"cachedNamespace\","
        + "        \"extractionNamespace\": {"
        + "          \"type\": \"jdbc\","
        + "          \"pollPeriod\": \"PT1H\","
        + "          \"connectorConfig\": {"
        + "            \"connectURI\": \"%s\""
        + "          },"
        + "          \"table\": \"%s\","
        + "          \"keyColumn\": \"country_code\","
        + "          \"valueColumn\": \"country_name\","
        + "          \"tsColumn\": \"created_date\""
        + "        },"
        + "        \"injective\": true,"
        + "        \"firstCacheTimeout\": 120000"
        + "      }"
        + "    }"
        + "  }"
        + "}";

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .addExtension(NamespaceExtractionModule.class)
                               .addServer(coordinator);
  }

  @Test
  public void test_derbyJdbcLookup() throws Exception
  {
    // Create a table in the metadata store to hold the lookup values
    final SQLMetadataConnector connector = coordinator.bindings().sqlMetadataConnector();
    connector.retryWithHandle(
        handle -> handle.update(
            "CREATE TABLE embedded_lookups("
            + "  created_date TIMESTAMP NOT NULL,\n"
            + "  country_code VARCHAR(10) NOT NULL,\n"
            + "  country_name VARCHAR(255) NOT NULL,\n"
            + "  PRIMARY KEY (country_code)"
            + ")"
        )
    );
    connector.retryWithHandle(
        handle -> handle.insert(
            "INSERT INTO embedded_lookups"
            + " (created_date, country_code, country_name) VALUES"
            + " ('2025-06-01 00:00:00', 'AU', 'Australia'),"
            + " ('2025-06-02 00:00:00', 'PR', 'Puerto Rico')"
        )
    );

    // Initialize lookups
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateAllLookups(Map.of())
    );

    // Create the lookup
    final String lookupName = dataSource;
    final String lookupPayload = StringUtils.format(
        BULK_UPDATE_LOOKUP_PAYLOAD,
        lookupName,
        ((TestDerbyConnector) connector).getJdbcUri(),
        JDBC_LOOKUP_TABLE
    );
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateAllLookups(EmbeddedClusterApis.deserializeJsonToMap(lookupPayload))
    );

    // Add the broker to the cluster and start it
    cluster.addServer(broker);
    broker.start();

    // Query the lookups
    Assertions.assertEquals(
        "Puerto Rico,Australia",
        cluster.runSql("SELECT LOOKUP('PR', '%1$s'), LOOKUP('AU', '%1$s')", dataSource)
    );
  }
}
