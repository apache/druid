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

import { Button, Classes, Dialog, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import type { Field } from '../../components';
import { AutoForm, ExternalLink, Loader } from '../../components';
import { useQueryManager } from '../../hooks';
import { getLink } from '../../links';
import { Api, AppToaster } from '../../singletons';
import { getDruidErrorMessage } from '../../utils';

interface CompactionDynamicConfig {
  compactionTaskSlotRatio: number;
  maxCompactionTaskSlots: number;
}

const COMPACTION_DYNAMIC_CONFIG_FIELDS: Field<CompactionDynamicConfig>[] = [
  {
    name: 'compactionTaskSlotRatio',
    type: 'ratio',
    defaultValue: 0.1,
    info: <>The ratio of the total task slots to the compaction task slots.</>,
  },
  {
    name: 'maxCompactionTaskSlots',
    type: 'number',
    defaultValue: 2147483647,
    info: <>The maximum number of task slots for compaction tasks</>,
  },
];

export interface CompactionDynamicConfigDialogProps {
  onClose(): void;
}

export const CompactionDynamicConfigDialog = React.memo(function CompactionDynamicConfigDialog(
  props: CompactionDynamicConfigDialogProps,
) {
  const { onClose } = props;
  const [dynamicConfig, setDynamicConfig] = useState<
    Partial<CompactionDynamicConfig> | undefined
  >();

  useQueryManager<null, Record<string, any>>({
    initQuery: null,
    processQuery: async () => {
      try {
        const c = (await Api.instance.get('/druid/coordinator/v1/config/compaction')).data;
        setDynamicConfig({
          compactionTaskSlotRatio: c.compactionTaskSlotRatio ?? 0.1,
          maxCompactionTaskSlots: c.maxCompactionTaskSlots ?? 2147483647,
        });
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
      // This API is terrible. https://druid.apache.org/docs/latest/operations/api-reference.html#automatic-compaction-configuration
      await Api.instance.post(
        `/druid/coordinator/v1/config/compaction/taskslots?ratio=${dynamicConfig.compactionTaskSlotRatio}&max=${dynamicConfig.maxCompactionTaskSlots}`,
        {},
      );
    } catch (e) {
      AppToaster.show({
        icon: IconNames.ERROR,
        intent: Intent.DANGER,
        message: `Could not save compaction dynamic config: ${getDruidErrorMessage(e)}`,
      });
    }

    AppToaster.show({
      message: 'Saved compaction dynamic config',
      intent: Intent.SUCCESS,
    });
    onClose();
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
                )}/operations/api-reference.html#automatic-compaction-configuration`}
              >
                documentation
              </ExternalLink>
              .
            </p>
            <AutoForm
              fields={COMPACTION_DYNAMIC_CONFIG_FIELDS}
              model={dynamicConfig}
              onChange={setDynamicConfig}
            />
          </div>
          <div className={Classes.DIALOG_FOOTER}>
            <div className={Classes.DIALOG_FOOTER_ACTIONS}>
              <Button
                text="Save"
                onClick={saveConfig}
                intent={Intent.PRIMARY}
                rightIcon={IconNames.TICK}
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
