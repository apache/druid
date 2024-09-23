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

import { Button, Icon, Intent, Menu, MenuDivider, MenuItem, Popover } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import copy from 'copy-to-clipboard';
import React, { useCallback, useState } from 'react';
import { useStore } from 'zustand';

import { Loader } from '../../../components';
import { useClock, useInterval, useQueryManager } from '../../../hooks';
import { Api, AppToaster } from '../../../singletons';
import { formatDuration, prettyFormatIsoDate } from '../../../utils';
import { CancelQueryDialog } from '../cancel-query-dialog/cancel-query-dialog';
import { DartDetailsDialog } from '../dart-details-dialog/dart-details-dialog';
import { workStateStore } from '../work-state-store';

import './current-dart-panel.scss';

interface CurrentDartEntry {
  sqlQueryId: string;
  sql: string;
  identity: string;
  startTime: string;
}

export interface CurrentViberPanelProps {
  onClose(): void;
}

export const CurrentDartPanel = React.memo(function CurrentViberPanel(
  props: CurrentViberPanelProps,
) {
  const { onClose } = props;

  const [showSql, setShowSql] = useState<string | undefined>();
  const [confirmCancelId, setConfirmCancelId] = useState<string | undefined>();

  const workStateVersion = useStore(
    workStateStore,
    useCallback(state => state.version, []),
  );

  const [currentDartState, queryManager] = useQueryManager<number, CurrentDartEntry[]>({
    query: workStateVersion,
    processQuery: async _ => {
      return (await Api.instance.get('/druid/v2/sql/dart')).data.queries;
    },
  });

  useInterval(() => {
    queryManager.rerunLastQuery(true);
  }, 3000);

  const now = useClock();

  const currentDart = currentDartState.getSomeData();
  return (
    <div className="current-dart-panel">
      <div className="title">
        Current Dart queries
        <Button className="close-button" icon={IconNames.CROSS} minimal onClick={onClose} />
      </div>
      {currentDart ? (
        <div className="work-entries">
          {currentDart.map(w => {
            const menu = (
              <Menu>
                <MenuItem
                  icon={IconNames.EYE_OPEN}
                  text="Show SQL"
                  onClick={() => {
                    setShowSql(w.sql);
                  }}
                />
                <MenuItem
                  icon={IconNames.DUPLICATE}
                  text="Copy ID"
                  onClick={() => {
                    copy(w.sqlQueryId, { format: 'text/plain' });
                    AppToaster.show({
                      message: `${w.sqlQueryId} copied to clipboard`,
                      intent: Intent.SUCCESS,
                    });
                  }}
                />
                <MenuDivider />
                <MenuItem
                  icon={IconNames.CROSS}
                  text="Cancel query"
                  intent={Intent.DANGER}
                  onClick={() => setConfirmCancelId(w.sqlQueryId)}
                />
              </Menu>
            );

            const duration = now.valueOf() - new Date(w.startTime).valueOf();

            return (
              <Popover className="work-entry" key={w.sqlQueryId} position="left" content={menu}>
                <div>
                  <div className="line1">
                    <Icon
                      className="status-icon"
                      icon={IconNames.ROCKET_SLANT}
                      style={{ color: '#2167d5' }}
                    />
                    <div className="timing">
                      {prettyFormatIsoDate(w.startTime) +
                        (duration > 0 ? ` (${formatDuration(duration)})` : '')}
                    </div>
                  </div>
                  <div className="line2">
                    <Icon className="output-icon" icon={IconNames.APPLICATION} />
                    <div className={classNames('output-datasource')}>select query</div>
                  </div>
                </div>
              </Popover>
            );
          })}
        </div>
      ) : currentDartState.isLoading() ? (
        <Loader />
      ) : undefined}
      {confirmCancelId && (
        <CancelQueryDialog
          // eslint-disable-next-line @typescript-eslint/no-misused-promises
          onCancel={async () => {
            if (!confirmCancelId) return;
            try {
              await Api.instance.delete(`/druid/v2/sql/dart/${Api.encodePath(confirmCancelId)}`);

              AppToaster.show({
                message: 'Query canceled',
                intent: Intent.SUCCESS,
              });
            } catch {
              AppToaster.show({
                message: 'Could not cancel query',
                intent: Intent.DANGER,
              });
            }
          }}
          onDismiss={() => setConfirmCancelId(undefined)}
        />
      )}
      {showSql && <DartDetailsDialog sql={showSql} onClose={() => setShowSql(undefined)} />}
    </div>
  );
});
