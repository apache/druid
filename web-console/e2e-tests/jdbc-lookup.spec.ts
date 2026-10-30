/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import type { APIRequestContext } from '@playwright/test';
import { L } from 'druid-query-toolkit';

import { LookupsOverview } from './component/lookups/overview';
import { expect, test } from './util/fixtures';
import { CLUSTER_STATE_POLL, querySql } from './util/sql';

// The JDBC URI of a database with a table of country codes and names, which the cluster can read. JdbcLookupWebConsoleTest
// (in embedded-tests) sets it up, in the cluster's (Derby) metadata store.
const CONNECT_URI = process.env['DRUID_E2E_TEST_LOOKUP_CONNECT_URI'];
const TABLE = process.env['DRUID_E2E_TEST_LOOKUP_TABLE'];

test.describe('JDBC lookup', () => {
  test.skip(
    !CONNECT_URI || !TABLE,
    'needs DRUID_E2E_TEST_LOOKUP_CONNECT_URI and DRUID_E2E_TEST_LOOKUP_TABLE (run by JdbcLookupWebConsoleTest in embedded-tests)',
  );

  test('Adds a JDBC lookup and queries it', async ({ page, request }) => {
    // Lookups are not deleted with the datasources, so the name is unique
    const lookupName = `country_names_${Date.now()}`;
    const lookupsOverview = new LookupsOverview(page);
    await lookupsOverview.addJdbcLookup({
      name: lookupName,
      connectUri: CONNECT_URI!,
      table: TABLE!,
      keyColumn: 'country_code',
      valueColumn: 'country_name',
      timestampColumn: 'created_date',
    });

    expect(await lookupsOverview.getLookups(lookupName)).toEqual([
      { name: lookupName, tier: '__default', type: 'cachedNamespace' },
    ]);

    // Once the Broker loads the lookup (from the table), queries can use it
    await expect
      .poll(() => lookUp(request, lookupName, ['PR', 'AU']), CLUSTER_STATE_POLL)
      .toEqual(['Puerto Rico', 'Australia']);
  });
});

/**
 * The values of the keys in the lookup, or null while the lookup can not be queried (yet).
 */
async function lookUp(
  request: APIRequestContext,
  lookupName: string,
  keys: string[],
): Promise<unknown[] | null> {
  try {
    const [row] = await querySql(
      request,
      `SELECT ${keys.map((key, i) => `LOOKUP(${L(key)}, ${L(lookupName)}) AS "v${i}"`).join(', ')}`,
    );
    return keys.map((_, i) => row[`v${i}`]);
  } catch {
    return null;
  }
}
