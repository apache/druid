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

import { Button, Callout, Checkbox, Classes, Dialog, InputGroup, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useMemo, useState } from 'react';

import { Loader } from '../../components';

import type { TieredServers } from './tiered-servers';

export interface ServerMultiSelectDialogProps {
  title: string;
  servers: TieredServers | undefined;
  selectedServers: string[];
  onSave(servers: string[]): void;
  onClose(): void;
}

export const ServerMultiSelectDialog = React.memo(function ServerMultiSelectDialog(
  props: ServerMultiSelectDialogProps,
) {
  const { title, servers, selectedServers: initialSelection, onSave, onClose } = props;
  const [selected, setSelected] = useState<Set<string>>(() => new Set(initialSelection));
  const [searchText, setSearchText] = useState('');

  const staleServers = useMemo(() => {
    if (!servers) return [];
    const allSet = new Set(servers.allServers);
    return initialSelection.filter(s => !allSet.has(s));
  }, [servers, initialSelection]);

  function toggleServer(server: string) {
    setSelected(prev => {
      const next = new Set(prev);
      if (next.has(server)) {
        next.delete(server);
      } else {
        next.add(server);
      }
      return next;
    });
  }

  function toggleTier(_tier: string, tierServers: string[]) {
    setSelected(prev => {
      const next = new Set(prev);
      const allSelected = tierServers.every(s => next.has(s));
      for (const s of tierServers) {
        if (allSelected) {
          next.delete(s);
        } else {
          next.add(s);
        }
      }
      return next;
    });
  }

  function removeStaleServers() {
    setSelected(prev => {
      const next = new Set(prev);
      for (const s of staleServers) {
        next.delete(s);
      }
      return next;
    });
  }

  const filteredTiers = useMemo(() => {
    const lowerSearch = searchText.toLowerCase();
    return servers
      ? servers.tiers.map(tier => {
          const tierServers = servers.serversByTier[tier] || [];
          const filtered = lowerSearch
            ? tierServers.filter(s => s.toLowerCase().includes(lowerSearch))
            : tierServers;
          return { tier, tierServers, filtered };
        })
      : [];
  }, [servers, searchText]);

  return (
    <Dialog className="server-multi-select-dialog" title={title} isOpen onClose={onClose}>
      {servers ? (
        <>
          <div className={Classes.DIALOG_BODY}>
            {staleServers.length > 0 && (
              <Callout intent={Intent.WARNING} style={{ marginBottom: 10 }}>
                {staleServers.length} selected server{staleServers.length > 1 ? 's are' : ' is'} no
                longer in the cluster
                {staleServers.length <= 5 ? `: ${staleServers.join(', ')}` : ''}.{' '}
                <Button minimal small intent={Intent.WARNING} onClick={removeStaleServers}>
                  Remove
                </Button>
              </Callout>
            )}
            <InputGroup
              leftIcon={IconNames.SEARCH}
              placeholder="Search servers..."
              value={searchText}
              onChange={e => setSearchText(e.target.value)}
              style={{ marginBottom: 10 }}
            />
            <div style={{ maxHeight: 400, overflowY: 'auto' }}>
              {filteredTiers.map(({ tier, tierServers, filtered: filteredServers }) => {
                if (filteredServers.length === 0) return null;

                const hiddenCount = tierServers.length - filteredServers.length;
                const allSelected = filteredServers.every(s => selected.has(s));
                const someSelected = !allSelected && filteredServers.some(s => selected.has(s));

                return (
                  <div key={tier} style={{ marginBottom: 8 }}>
                    <Checkbox
                      checked={allSelected}
                      indeterminate={someSelected}
                      labelElement={
                        <>
                          <strong>{tier}</strong>
                          {hiddenCount > 0 && (
                            <span style={{ fontWeight: 'normal', opacity: 0.6 }}>
                              {' '}
                              ({hiddenCount} hidden by filter)
                            </span>
                          )}
                        </>
                      }
                      onChange={() => toggleTier(tier, filteredServers)}
                    />
                    <div style={{ paddingLeft: 20 }}>
                      {filteredServers.map(server => (
                        <Checkbox
                          key={server}
                          checked={selected.has(server)}
                          label={server}
                          onChange={() => toggleServer(server)}
                        />
                      ))}
                    </div>
                  </div>
                );
              })}
              {servers.allServers.length === 0 && (
                <Callout intent={Intent.NONE}>No historical servers found in the cluster.</Callout>
              )}
            </div>
          </div>
          <div className={Classes.DIALOG_FOOTER}>
            <div className={Classes.DIALOG_FOOTER_ACTIONS}>
              <span style={{ flex: 1, alignSelf: 'center' }}>
                {selected.size} server{selected.size !== 1 ? 's' : ''} selected
              </span>
              <Button text="Cancel" onClick={onClose} />
              <Button
                text="Save"
                intent={Intent.PRIMARY}
                onClick={() => {
                  onSave(Array.from(selected).sort());
                  onClose();
                }}
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
