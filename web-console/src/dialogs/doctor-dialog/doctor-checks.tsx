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

import axios from 'axios';

import { pluralIfNeeded, queryDruidSql } from '../../utils';
import { deepGet } from '../../utils/object-change';
import { postToSampler } from '../../utils/sampler';

export interface CheckControls {
  addSuggestion: (message: string) => void;
  addIssue: (message: string) => void;
  terminateChecks: () => void;
}

export interface DoctorCheck {
  name: string;
  check: (controls: CheckControls) => Promise<void>;
}

const RUNTIME_PROPERTIES_ALL_NODES_MUST_AGREE_ON: string[] = [
  'user.timezone',
  'druid.zk.service.host',
];

// In the future (when we can query other nodes) is will also be cool to check:
// 'druid.storage.type' <=> historicals, overlords, mm
// 'druid.indexer.logs.type' <=> overlord, mm, + peons

const RUNTIME_PROPERTIES_MASTER_NODES_SHOULD_AGREE_ON: string[] = [
  'druid.metadata.storage.type', // overlord + coordinator
  'druid.metadata.storage.connector.connectURI',
];

export const DOCTOR_CHECKS: DoctorCheck[] = [
  // -------------------------------------
  // Self (router) checks
  // -------------------------------------
  {
    name: 'Verify own status',
    check: async controls => {
      // Make sure that the router responds to /status and gives some valid info back
      let status: any;
      try {
        status = (await axios.get(`/status`)).data;
      } catch (e) {
        controls.addIssue(
          `Did not get a /status response from the Router node. Try confirming that it is running and accessible. Got: ${e.message}`,
        );
        controls.terminateChecks();
        return;
      }

      if (typeof status.version !== 'string') {
        controls.addIssue('Could not get a valid /status response from the Router.');
      }
    },
  },
  {
    name: 'Verify own runtime properties',
    check: async controls => {
      // Make sure that everything in /status/properties is above board
      let properties: Record<string, string>;
      try {
        properties = (await axios.get(`/status/properties`)).data;
      } catch (e) {
        controls.addIssue(
          `Did not get a /status/properties response from the Router. Message: ${e.message}`,
        );
        return;
      }

      // Check that the management proxy is on, it really should be for someone to access the console in the first place but everything could happen
      if (properties['druid.router.managementProxy.enabled'] !== 'true') {
        controls.addIssue(
          `The Router's "druid.router.managementProxy.enabled" is not reported as "true". This means that the Coordinator and Overlord will not be accessible from the Router (and this console).`,
        );
      }

      // Check that the underlying Java is Java 8 the only officially supported Java version at the moment.
      if (
        properties['java.specification.version'] &&
        properties['java.specification.version'] !== '1.8'
      ) {
        controls.addSuggestion(
          `It looks like are running Java ${properties['java.runtime.version']}. Druid only officially supports Java 1.8.x`,
        );
      }

      // Check "file.encoding"
      if (properties['file.encoding'] && properties['file.encoding'] !== 'UTF-8') {
        controls.addSuggestion(
          `It looks like "file.encoding" is set to ${properties['file.encoding']}, it is recommended to set this to "UTF-8"`,
        );
      }

      // Check "user.timezone"
      if (properties['user.timezone'] && properties['user.timezone'] !== 'UTC') {
        controls.addSuggestion(
          `It looks like "user.timezone" is set to ${properties['user.timezone']}, it is recommended to set this to "UTC"`,
        );
      }
    },
  },

  // -------------------------------------
  // Coordinator and Overlord
  // -------------------------------------
  {
    name: 'Verify the Coordinator and Overlord status',
    check: async controls => {
      // Make sure that everything in Coordinator's /status is good
      let myStatus: any;
      try {
        myStatus = (await axios.get(`/status`)).data;
      } catch {
        return;
      }

      let coordinatorStatus: any;
      try {
        coordinatorStatus = (await axios.get(`/proxy/coordinator/status`)).data;
      } catch (e) {
        controls.addIssue(
          'Did not get a /status response from the Coordinator node. Try confirming that it is running and accessible.',
        );
        return;
      }

      let overlordStatus: any;
      try {
        overlordStatus = (await axios.get(`/proxy/overlord/status`)).data;
      } catch (e) {
        controls.addIssue(
          'Did not get a /status response from the Overlord node. Try confirming that it is running and accessible.',
        );
        return;
      }

      if (myStatus.version !== coordinatorStatus.version) {
        controls.addSuggestion(
          `It looks like the Router and Coordinator nodes are on different versions of Druid. This may indicate a problem if you are not in the middle of a rolling upgrade.`,
        );
      }

      if (myStatus.version !== overlordStatus.version) {
        controls.addSuggestion(
          `It looks like the Router and Overlord nodes are on different versions of Druid. This may indicate a problem if you are not in the middle of a rolling upgrade.`,
        );
      }
    },
  },
  {
    name: 'Verify the Coordinator and Overlord runtime properties',
    check: async controls => {
      // Make sure that everything in coordinator and overlord /status/properties is good and matches where needed
      let myProperties: Record<string, string>;
      try {
        myProperties = (await axios.get(`/status/properties`)).data;
      } catch {
        return;
      }

      let coordinatorProperties: Record<string, string>;
      try {
        coordinatorProperties = (await axios.get(`/proxy/coordinator/status/properties`)).data;
      } catch (e) {
        controls.addIssue(
          'Did not get a /status response from the coordinator. Try confirming that it is running and accessible.',
        );
        return;
      }

      let overlordProperties: Record<string, string>;
      try {
        overlordProperties = (await axios.get(`/proxy/overlord/status/properties`)).data;
      } catch (e) {
        controls.addIssue(
          'Did not get a /status response from the overlord. Try confirming that it is running and accessible.',
        );
        return;
      }

      for (const prop of RUNTIME_PROPERTIES_ALL_NODES_MUST_AGREE_ON) {
        if (myProperties[prop] !== coordinatorProperties[prop]) {
          controls.addIssue(
            `The Router and Coordinator do not agree on the "${prop}" runtime property ("${myProperties[prop]}" vs "${coordinatorProperties[prop]}")`,
          );
        }
        if (myProperties[prop] !== overlordProperties[prop]) {
          controls.addIssue(
            `The Router and Overlord do not agree on the "${prop}" runtime property ("${myProperties[prop]}" vs "${overlordProperties[prop]}")`,
          );
        }
      }

      for (const prop of RUNTIME_PROPERTIES_MASTER_NODES_SHOULD_AGREE_ON) {
        if (coordinatorProperties[prop] !== overlordProperties[prop]) {
          controls.addSuggestion(
            `The Coordinator and Overlord do not agree on the "${prop}" runtime property ("${coordinatorProperties[prop]}" vs "${overlordProperties[prop]}")`,
          );
        }
      }
    },
  },

  // -------------------------------------
  // Check sampler
  // -------------------------------------
  {
    name: 'Verify that the sampler works',
    check: async controls => {
      // Make sure that everything in Coordinator's /status is good
      let testSampledData: any;
      try {
        testSampledData = await postToSampler(
          {
            type: 'index',
            spec: {
              type: 'index',
              ioConfig: { type: 'index', firehose: { type: 'inline', data: '{"test":"Data"}' } },
              dataSchema: {
                dataSource: 'sample',
                parser: {
                  type: 'string',
                  parseSpec: {
                    format: 'json',
                    timestampSpec: {
                      column: '!!!_no_such_column_!!!',
                      missingValue: '2010-01-01T00:00:00Z',
                    },
                    dimensionsSpec: { dimensions: ['test'] },
                  },
                },
                transformSpec: {},
                metricsSpec: [],
                granularitySpec: { queryGranularity: 'NONE' },
              },
            },
            samplerConfig: {
              numRows: 50,
              timeoutMs: 1000,
            },
          },
          'doctor',
        );
      } catch {
        controls.addIssue(`Could not use the sampler.`);
        return;
      }

      if (deepGet(testSampledData, 'data.0.parsed.test') !== 'Data') {
        controls.addIssue(`Sampler returned incorrect data.`);
      }
    },
  },

  // -------------------------------------
  // Check SQL
  // -------------------------------------
  {
    name: 'Verify that SQL works',
    check: async controls => {
      // Make sure that we can run the simplest query
      let sqlResult: any[];
      try {
        sqlResult = await queryDruidSql({ query: `SELECT 1 + 1 AS "two"` });
      } catch (e) {
        controls.addIssue(
          `Could not query SQL ensure that "druid.sql.enable" is set to "true" and that there is a Broker node running. Got: ${e.message}`,
        );
        controls.terminateChecks();
        return;
      }

      if (sqlResult.length !== 1 || sqlResult[0]['two'] !== 2) {
        controls.addIssue(`Got incorrect results from a basic SQL query.`);
      }
    },
  },
  {
    name: 'Verify that there are historical nodes',
    check: async controls => {
      // Make sure that there are broker and historical nodes reported from sys.servers
      let sqlResult: any[];
      try {
        sqlResult = await queryDruidSql({
          query: `SELECT
  COUNT(*) AS "historicals"
FROM sys.servers
WHERE "server_type" = 'historical'`,
        });
      } catch (e) {
        controls.addIssue(`Could not run a sys.servers query. Got: ${e.message}`);
        return;
      }

      if (sqlResult.length === 1 && sqlResult[0]['historicals'] === 0) {
        controls.addIssue(`There do not appear to be any historical nodes.`);
      }
    },
  },
  {
    name: 'Verify that the historicals are not overfilled',
    check: async controls => {
      // Make sure that no nodes are reported that are over 95% capacity
      let sqlResult: any[];
      try {
        sqlResult = await queryDruidSql({
          query: `SELECT
  "server",
  "curr_size" * 1.0 / "max_size" AS "fill"
FROM sys.servers
WHERE "server_type" = 'historical' AND "curr_size" * 1.0 / "max_size" > 0.9
ORDER BY "server" DESC`,
        });
      } catch (e) {
        controls.addIssue(`Could not run a sys.servers query. Got: ${e.message}`);
        return;
      }

      function formatPercent(server: any): string {
        return (server['fill'] * 100).toFixed(2);
      }

      for (const server of sqlResult) {
        if (server['fill'] > 0.95) {
          controls.addIssue(
            `Server "${server['server']}" appears to be over 95% full (is ${formatPercent(
              server,
            )}%). Increase capacity.`,
          );
        } else {
          controls.addSuggestion(
            `Server "${server['server']}" appears to be over 90% full (is ${formatPercent(
              server,
            )}%)`,
          );
        }
      }
    },
  },
  {
    name: 'Look for time chunks that could benefit from compaction',
    check: async controls => {
      // Check for any time chunks where there is more than 1 segment and avg segment size is less than 100MB
      const dayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
      let sqlResult: any[];
      try {
        sqlResult = await queryDruidSql({
          query: `SELECT
  "datasource",
  COUNT(*) AS "num_bad_time_chunks"
FROM (
  SELECT
    "datasource", "start", "end",
    AVG("size") AS "avg_segment_size_in_time_chunk",
    SUM("size") AS "total_size",
    COUNT(*) AS "num_segments"
  FROM sys.segments
  WHERE is_published = 1 AND "start" < '${dayAgo}'
  GROUP BY 1, 2, 3
  HAVING "num_segments" > 1 AND "total_size" > 1 AND "avg_segment_size_in_time_chunk" < 100000000
) 
GROUP BY 1
ORDER BY "num_bad_time_chunks"`,
        });
      } catch (e) {
        return;
      }

      if (sqlResult.length) {
        // Grab the auto-compaction definitions and ignore dataSources that already have auto-compaction
        let compactionResult: any;
        try {
          compactionResult = (await axios.get('/druid/coordinator/v1/config/compaction')).data;
        } catch (e) {
          controls.addIssue(`Could not get compaction config. Something is wrong.`);
          return;
        }

        if (!compactionResult.compactionConfigs) return;

        if (!Array.isArray(compactionResult.compactionConfigs)) {
          controls.addIssue(`Got invalid value from compaction config. Something is wrong.`);
          return;
        }

        const dataSourcesWithCompaction = compactionResult.compactionConfigs.map(
          (d: any) => d.dataSource,
        );

        sqlResult = sqlResult.filter(d => !dataSourcesWithCompaction.includes(d['datasource']));

        for (const datasource of sqlResult) {
          controls.addSuggestion(
            `Datasource "${
              datasource['datasource']
            }" could benefit from auto-compaction as it has ${pluralIfNeeded(
              datasource['num_bad_time_chunks'],
              'time chunk',
            )} that have multiple small segments that could be compacted.`,
          );
        }
      }
    },
  },
];
