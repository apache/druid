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

import { Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import { AutoForm, ExternalLink, Loader } from '../../components';
import { OVERLORD_DYNAMIC_CONFIG_FIELDS, OverlordDynamicConfig } from '../../druid-models';
import { useQueryManager } from '../../hooks';
import { getLink } from '../../links';
import { Api, AppToaster } from '../../singletons';
import { getDruidErrorMessage } from '../../utils';
import { SnitchDialog } from '..';

import './overlord-dynamic-config-dialog.scss';

export interface OverlordDynamicConfigDialogProps {
  onClose(): void;
}

export const OverlordDynamicConfigDialog = React.memo(function OverlordDynamicConfigDialog(
  props: OverlordDynamicConfigDialogProps,
) {
  const { onClose } = props;
  const [dynamicConfig, setDynamicConfig] = useState<OverlordDynamicConfig | undefined>();

  const [historyRecordsState] = useQueryManager<null, any[]>({
    initQuery: null,
    processQuery: async () => {
      const historyResp = await Api.instance.get(`/druid/indexer/v1/worker/history?count=100`);
      return historyResp.data;
    },
  });

  useQueryManager<null, Record<string, any>>({
    initQuery: null,
    processQuery: async () => {
      try {
        const configResp = await Api.instance.get(`/druid/indexer/v1/worker`);
        setDynamicConfig(configResp.data || {});
      } catch (e) {
        AppToaster.show({
          icon: IconNames.ERROR,
          intent: Intent.DANGER,
          message: `Could not load overlord dynamic config: ${getDruidErrorMessage(e)}`,
        });
        onClose();
      }
      return {};
    },
  });

  async function saveConfig(comment: string) {
    try {
      await Api.instance.post('/druid/indexer/v1/worker', dynamicConfig, {
        headers: {
          'X-Druid-Author': 'console',
          'X-Druid-Comment': comment,
        },
      });
    } catch (e) {
      AppToaster.show({
        icon: IconNames.ERROR,
        intent: Intent.DANGER,
        message: `Could not save overlord dynamic config: ${getDruidErrorMessage(e)}`,
      });
    }

    AppToaster.show({
      message: 'Saved overlord dynamic config',
      intent: Intent.SUCCESS,
    });
    onClose();
  }

  return (
    <SnitchDialog
      className="overlord-dynamic-config-dialog"
      onSave={saveConfig}
      onClose={onClose}
      title="Overlord dynamic config"
      historyRecords={historyRecordsState.data}
    >
      {dynamicConfig ? (
        <>
          <p>
            Edit the overlord dynamic configuration on the fly. For more information please refer to
            the{' '}
            <ExternalLink
              href={`${getLink('DOCS')}/configuration/index.html#overlord-dynamic-configuration`}
            >
              documentation
            </ExternalLink>
            .
          </p>
          <AutoForm
            fields={OVERLORD_DYNAMIC_CONFIG_FIELDS}
            model={dynamicConfig}
            onChange={setDynamicConfig}
          />
        </>
      ) : (
        <Loader />
      )}
    </SnitchDialog>
  );
});
