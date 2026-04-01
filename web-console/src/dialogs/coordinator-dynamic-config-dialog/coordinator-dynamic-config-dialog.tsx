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

import { Code, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useMemo, useState } from 'react';

import type { Field, FormJsonTabs } from '../../components';
import { AutoForm, ExternalLink, FormJsonSelector, JsonInput, Loader } from '../../components';
import type { CoordinatorDynamicConfig } from '../../druid-models';
import {
  cloneCountSummary,
  COORDINATOR_DYNAMIC_CONFIG_FIELDS,
  serverCountSummary,
} from '../../druid-models';
import type { Capabilities } from '../../helpers';
import { useQueryManager } from '../../hooks';
import { getLink } from '../../links';
import { Api, AppToaster } from '../../singletons';
import { filterMap, getApiArray, getDruidErrorMessage, queryDruidSql } from '../../utils';
import { SnitchDialog } from '..';

import { CloneServerMappingDialog } from './clone-server-mapping-dialog';
import { COORDINATOR_DYNAMIC_CONFIG_COMPLETIONS } from './coordinator-dynamic-config-completions';
import { ServerMultiSelectDialog } from './server-multi-select-dialog';
import type { TieredServers } from './tiered-servers';

import './coordinator-dynamic-config-dialog.scss';

export interface CoordinatorDynamicConfigDialogProps {
  capabilities: Capabilities;
  onClose(): void;
}

function buildTieredServers(rows: { server: string; tier: string }[]): TieredServers {
  const serversByTier: Record<string, string[]> = {};
  const serverToTier: Record<string, string> = {};
  for (const row of rows) {
    if (!serversByTier[row.tier]) {
      serversByTier[row.tier] = [];
    }
    serversByTier[row.tier].push(row.server);
    serverToTier[row.server] = row.tier;
  }
  const tiers = Object.keys(serversByTier).sort();
  for (const tier of tiers) {
    serversByTier[tier].sort();
  }
  const allServers = tiers.flatMap(t => serversByTier[t]);
  return { tiers, serversByTier, serverToTier, allServers };
}

function buildServerPickerFields(
  servers: TieredServers | undefined,
): Field<CoordinatorDynamicConfig>[] {
  return [
    {
      name: 'decommissioningNodes',
      type: 'custom',
      emptyValue: [],
      info: (
        <>
          List of historical services to &apos;decommission&apos;. Coordinator will not assign new
          segments to &apos;decommissioning&apos; services, and segments will be moved away from
          them to be placed on non-decommissioning services at the maximum rate specified by{' '}
          <Code>maxSegmentsToMove</Code>.
        </>
      ),
      customSummary: serverCountSummary,
      customDialog: ({ value, onValueChange, onClose }) => (
        <ServerMultiSelectDialog
          title="Decommissioning nodes"
          servers={servers}
          selectedServers={value || []}
          onSave={v => onValueChange(v)}
          onClose={onClose}
        />
      ),
    },
    {
      name: 'turboLoadingNodes',
      type: 'custom',
      experimental: true,
      info: (
        <>
          <p>
            List of Historical servers to place in turbo loading mode. These servers use a larger
            thread-pool to load segments faster but at the cost of query performance. For servers
            specified in <Code>turboLoadingNodes</Code>,{' '}
            <Code>druid.coordinator.loadqueuepeon.http.batchSize</Code> is ignored and the
            coordinator uses the value of the respective <Code>numLoadingThreads</Code> instead.
          </p>
          <p>
            Please use this config with caution. All servers should eventually be removed from this
            list once the segment loading on the respective historicals is finished.
          </p>
        </>
      ),
      customSummary: serverCountSummary,
      customDialog: ({ value, onValueChange, onClose }) => (
        <ServerMultiSelectDialog
          title="Turbo loading nodes"
          servers={servers}
          selectedServers={value || []}
          onSave={v => onValueChange(v)}
          onClose={onClose}
        />
      ),
    },
    {
      name: 'cloneServers',
      type: 'custom',
      experimental: true,
      info: (
        <>
          <p>
            Map from target Historical server to source Historical server. The target clones all
            segments from the source, becoming an exact copy. The target does not participate in
            regular segment assignment or balancing, and its segments do not count towards replica
            counts.
          </p>
          <p>
            If the source server disappears, the target remains in the last known state of the
            source until removed from this mapping.
          </p>
        </>
      ),
      customSummary: cloneCountSummary,
      customDialog: ({ value, onValueChange, onClose }) => (
        <CloneServerMappingDialog
          servers={servers}
          cloneServers={value || {}}
          onSave={v => onValueChange(v)}
          onClose={onClose}
        />
      ),
    },
  ];
}

export const CoordinatorDynamicConfigDialog = React.memo(function CoordinatorDynamicConfigDialog(
  props: CoordinatorDynamicConfigDialogProps,
) {
  const { capabilities, onClose } = props;
  const [currentTab, setCurrentTab] = useState<FormJsonTabs>('form');
  const [dynamicConfig, setDynamicConfig] = useState<CoordinatorDynamicConfig | undefined>();
  const [jsonError, setJsonError] = useState<Error | undefined>();

  const [historyRecordsState] = useQueryManager<null, any[]>({
    initQuery: null,
    processQuery: async (_, signal) => {
      return await getApiArray(`/druid/coordinator/v1/config/history?count=100`, signal);
    },
  });

  useQueryManager<null, Record<string, any>>({
    initQuery: null,
    processQuery: async (_, signal) => {
      try {
        const configResp = await Api.instance.get('/druid/coordinator/v1/config', { signal });
        setDynamicConfig(configResp.data || {});
      } catch (e) {
        AppToaster.show({
          icon: IconNames.ERROR,
          intent: Intent.DANGER,
          message: `Could not load coordinator dynamic config: ${getDruidErrorMessage(e)}`,
        });
        onClose();
      }
      return {};
    },
  });

  const [serversState] = useQueryManager<Capabilities, TieredServers>({
    initQuery: capabilities,
    processQuery: async (capabilities, signal) => {
      if (capabilities.hasSql()) {
        const sqlResp = await queryDruidSql<{ server: string; tier: string }>(
          {
            query: `SELECT "server", "tier"
FROM "sys"."servers"
WHERE "server_type" = 'historical'
ORDER BY "tier", "server"`,
            context: { engine: 'native' },
          },
          signal,
        );
        return buildTieredServers(sqlResp);
      } else if (capabilities.hasCoordinatorAccess()) {
        const servers = await getApiArray('/druid/coordinator/v1/servers?simple', signal);
        const rows = filterMap(servers, (s: any) =>
          s.type === 'historical' ? { server: s.host, tier: s.tier } : undefined,
        );
        return buildTieredServers(rows);
      } else {
        throw new Error('Must have SQL or coordinator access');
      }
    },
  });

  const fields = useMemo(() => {
    const insertIndex = COORDINATOR_DYNAMIC_CONFIG_FIELDS.findIndex(
      f => f.name === 'killDataSourceWhitelist',
    );
    return [
      ...COORDINATOR_DYNAMIC_CONFIG_FIELDS.slice(0, insertIndex),
      ...buildServerPickerFields(serversState.data),
      ...COORDINATOR_DYNAMIC_CONFIG_FIELDS.slice(insertIndex),
    ];
  }, [serversState.data]);

  async function saveConfig(comment: string) {
    try {
      await Api.instance.post('/druid/coordinator/v1/config', dynamicConfig, {
        headers: {
          'X-Druid-Author': 'console',
          'X-Druid-Comment': comment,
        },
      });
    } catch (e) {
      AppToaster.show({
        icon: IconNames.ERROR,
        intent: Intent.DANGER,
        message: `Could not save coordinator dynamic config: ${getDruidErrorMessage(e)}`,
      });
    }

    AppToaster.show({
      message: 'Saved coordinator dynamic config',
      intent: Intent.SUCCESS,
    });
    onClose();
  }

  return (
    <SnitchDialog
      className="coordinator-dynamic-config-dialog"
      saveDisabled={Boolean(jsonError)}
      onSave={comment => void saveConfig(comment)}
      onClose={onClose}
      title="Coordinator dynamic config"
      historyRecords={historyRecordsState.data}
    >
      {dynamicConfig ? (
        <>
          <p>
            Edit the coordinator dynamic configuration on the fly. For more information please refer
            to the{' '}
            <ExternalLink href={`${getLink('DOCS')}/configuration/#dynamic-configuration`}>
              documentation
            </ExternalLink>
            .
          </p>
          <FormJsonSelector
            tab={currentTab}
            onChange={t => {
              setJsonError(undefined);
              setCurrentTab(t);
            }}
          />
          {currentTab === 'form' ? (
            <AutoForm fields={fields} model={dynamicConfig} onChange={setDynamicConfig} />
          ) : (
            <JsonInput
              value={dynamicConfig}
              height="50vh"
              onChange={setDynamicConfig}
              setError={setJsonError}
              jsonCompletions={COORDINATOR_DYNAMIC_CONFIG_COMPLETIONS}
            />
          )}
        </>
      ) : (
        <Loader />
      )}
    </SnitchDialog>
  );
});
