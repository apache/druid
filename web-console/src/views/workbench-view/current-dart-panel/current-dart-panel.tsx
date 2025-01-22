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
import { type IconName, IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import copy from 'copy-to-clipboard';
import React, { useState } from 'react';
import { useStore } from 'zustand';

import { Loader } from '../../../components';
import type { DartQueryEntry } from '../../../druid-models';
import { useClock, useInterval, useQueryManager } from '../../../hooks';
import { Api, AppToaster } from '../../../singletons';
import { formatDuration, prettyFormatIsoDate } from '../../../utils';
import { CancelQueryDialog } from '../cancel-query-dialog/cancel-query-dialog';
import { DartDetailsDialog } from '../dart-details-dialog/dart-details-dialog';
import { getMsqDartVersion, WORK_STATE_STORE } from '../work-state-store';

import './current-dart-panel.scss';

function stateToIconAndColor(status: DartQueryEntry['state']): [IconName, string] {
  switch (status) {
    case 'RUNNING':
      return [IconNames.REFRESH, '#2167d5'];
    case 'ACCEPTED':
      return [IconNames.CIRCLE, '#8d8d8d'];
    case 'CANCELED':
      return [IconNames.DISABLE, '#8d8d8d'];
    default:
      return [IconNames.CIRCLE, '#8d8d8d'];
  }
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

  const [dartQueryEntriesState, queryManager] = useQueryManager<number, DartQueryEntry[]>({
    query: useStore(WORK_STATE_STORE, getMsqDartVersion),
    processQuery: async (_, cancelToken) => {
      return (await Api.instance.get('/druid/v2/sql/dart', { cancelToken })).data.queries;
    },
  });

  useInterval(() => {
    queryManager.rerunLastQuery(true);
  }, 3000);

  const now = useClock();

  const dartQueryEntries = dartQueryEntriesState.getSomeData();
  return (
    <div className="current-dart-panel">
      <div className="title">
        Current Dart queries
        <Button className="close-button" icon={IconNames.CROSS} minimal onClick={onClose} />
      </div>
      {dartQueryEntries ? (
        <div className="work-entries">
          {dartQueryEntries.map(w => {
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
                  text="Copy SQL ID"
                  onClick={() => {
                    copy(w.sqlQueryId, { format: 'text/plain' });
                    AppToaster.show({
                      message: `${w.sqlQueryId} copied to clipboard`,
                      intent: Intent.SUCCESS,
                    });
                  }}
                />
                <MenuItem
                  icon={IconNames.DUPLICATE}
                  text="Copy Dart ID"
                  onClick={() => {
                    copy(w.dartQueryId, { format: 'text/plain' });
                    AppToaster.show({
                      message: `${w.dartQueryId} copied to clipboard`,
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

            const [icon, color] = stateToIconAndColor(w.state);
            const anonymous = w.identity === 'allowAll' && w.authenticator === 'allowAll';
            return (
              <Popover className="work-entry" key={w.sqlQueryId} position="left" content={menu}>
                <div onDoubleClick={() => setShowSql(w.sql)}>
                  <div className="line1">
                    <Icon
                      className={'status-icon ' + w.state.toLowerCase()}
                      icon={icon}
                      style={{ color }}
                      data-tooltip={`State: ${w.state}`}
                    />
                    <div className="timing">
                      {prettyFormatIsoDate(w.startTime) +
                        ((w.state === 'RUNNING' || w.state === 'ACCEPTED') && duration > 0
                          ? ` (${formatDuration(duration)})`
                          : '')}
                    </div>
                  </div>
                  <div className="line2">
                    <Icon className="identity-icon" icon={IconNames.MUGSHOT} />
                    <div
                      className={classNames('identity-identity', { anonymous })}
                      data-tooltip={`Identity: ${w.identity}\nAuthenticator: ${w.authenticator}`}
                    >
                      {anonymous ? 'anonymous' : `${w.identity} (${w.authenticator})`}
                    </div>
                  </div>
                </div>
              </Popover>
            );
          })}
        </div>
      ) : dartQueryEntriesState.isLoading() ? (
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
              queryManager.rerunLastQuery();
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
