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

import { Button, Classes, Code, Dialog, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import type { FormJsonTabs } from '../../components';
import { AutoForm, ExternalLink, FormJsonSelector, JsonInput, Loader } from '../../components';
import type { CompactionDynamicConfig } from '../../druid-models';
import {
  COMPACTION_DYNAMIC_CONFIG_DEFAULT_MAX,
  COMPACTION_DYNAMIC_CONFIG_DEFAULT_RATIO,
  COMPACTION_DYNAMIC_CONFIG_FIELDS,
} from '../../druid-models';
import { useQueryManager } from '../../hooks';
import { getLink } from '../../links';
import { Api, AppToaster } from '../../singletons';
import { getDruidErrorMessage, wait } from '../../utils';

import { COMPACTION_DYNAMIC_CONFIG_COMPLETIONS } from './compaction-dynamic-config-completions';

export interface CompactionDynamicConfigDialogProps {
  onClose(): void;
}

export const CompactionDynamicConfigDialog = React.memo(function CompactionDynamicConfigDialog(
  props: CompactionDynamicConfigDialogProps,
) {
  const { onClose } = props;
  const [currentTab, setCurrentTab] = useState<FormJsonTabs>('form');
  const [dynamicConfig, setDynamicConfig] = useState<
    Partial<CompactionDynamicConfig> | undefined
  >();
  const [jsonError, setJsonError] = useState<Error | undefined>();

  useQueryManager<null, Record<string, any>>({
    initQuery: null,
    processQuery: async (_, signal) => {
      try {
        const configResp = await Api.instance.get('/druid/indexer/v1/compaction/config/cluster', {
          signal,
        });
        setDynamicConfig(configResp.data || {});
      } catch (e) {
        AppToaster.show({
          icon: IconNames.ERROR,
          intent: Intent.DANGER,
          message: `Could not load compaction dynamic config: ${getDruidErrorMessage(e)}`,
        });
        onClose();
      }
      return {};
    },
  });

  async function saveConfig() {
    if (!dynamicConfig) return;
    try {
      await Api.instance.post('/druid/indexer/v1/compaction/config/cluster', dynamicConfig);
    } catch (e) {
      AppToaster.show({
        icon: IconNames.ERROR,
        intent: Intent.DANGER,
        message: `Could not save compaction dynamic config: ${getDruidErrorMessage(e)}`,
      });
      return;
    }

    AppToaster.show({
      message: 'Saved compaction dynamic config',
      intent: Intent.SUCCESS,
    });

    onClose();

    // Reload the page also because the datasources page pulls from different APIs depending on the setting of supervisor based compaction
    if (location.hash.includes('#datasources')) {
      await wait(1000); // Wait for a second to give the user time to read the toast
      location.reload();
    }
  }

  return (
    <Dialog
      className="compaction-dynamic-config-dialog"
      onClose={onClose}
      title="Compaction dynamic config"
      isOpen
    >
      {dynamicConfig ? (
        <>
          <div className={Classes.DIALOG_BODY}>
            <p>
              Edit the compaction dynamic configuration on the fly. For more information please
              refer to the{' '}
              <ExternalLink
                href={`${getLink(
                  'DOCS',
                )}/operations/api-reference#automatic-compaction-configuration`}
              >
                documentation
              </ExternalLink>
              .
            </p>
            <p>
              The maximum number of task slots used for compaction will be{' '}
              <Code>{`clamp(floor(${
                dynamicConfig.compactionTaskSlotRatio ?? COMPACTION_DYNAMIC_CONFIG_DEFAULT_RATIO
              } * total_task_slots), ${dynamicConfig.engine === 'msq' ? 2 : 1}, ${
                dynamicConfig.maxCompactionTaskSlots ?? COMPACTION_DYNAMIC_CONFIG_DEFAULT_MAX
              })`}</Code>
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
              <AutoForm
                fields={COMPACTION_DYNAMIC_CONFIG_FIELDS}
                model={dynamicConfig}
                onChange={setDynamicConfig}
              />
            ) : (
              <JsonInput
                value={dynamicConfig}
                height="50vh"
                onChange={setDynamicConfig}
                setError={setJsonError}
                jsonCompletions={COMPACTION_DYNAMIC_CONFIG_COMPLETIONS}
              />
            )}
          </div>
          <div className={Classes.DIALOG_FOOTER}>
            <div className={Classes.DIALOG_FOOTER_ACTIONS}>
              <Button
                text="Save"
                onClick={() => void saveConfig()}
                intent={Intent.PRIMARY}
                rightIcon={IconNames.TICK}
                disabled={Boolean(jsonError)}
              />
            </div>
          </div>
        </>
      ) : (
        <Loader />
      )}
    </Dialog>
  );
});
