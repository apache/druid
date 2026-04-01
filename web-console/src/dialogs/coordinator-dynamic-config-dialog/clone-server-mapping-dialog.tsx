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

import { Button, Callout, Classes, Dialog, Intent, MenuDivider, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Select } from '@blueprintjs/select';
import React, { useState } from 'react';

import { Loader } from '../../components';

import type { TieredServers } from './tiered-servers';

interface CloneMapping {
  target: string;
  source: string;
}

export interface CloneServerMappingDialogProps {
  servers: TieredServers | undefined;
  cloneServers: Record<string, string>;
  onSave(mapping: Record<string, string>): void;
  onClose(): void;
}

export const CloneServerMappingDialog = React.memo(function CloneServerMappingDialog(
  props: CloneServerMappingDialogProps,
) {
  const { servers, cloneServers: initialMapping, onSave, onClose } = props;
  const [mappings, setMappings] = useState<CloneMapping[]>(() =>
    Object.entries(initialMapping || {}).map(([target, source]) => ({ target, source })),
  );

  function updateMapping(index: number, field: 'target' | 'source', value: string) {
    setMappings(prev => prev.map((m, i) => (i === index ? { ...m, [field]: value } : m)));
  }

  function removeMapping(index: number) {
    setMappings(prev => prev.filter((_, i) => i !== index));
  }

  function addMapping() {
    setMappings(prev => [...prev, { target: '', source: '' }]);
  }

  function handleSave() {
    const result: Record<string, string> = {};
    for (const m of mappings) {
      if (m.target && m.source && m.target !== m.source) {
        result[m.target] = m.source;
      }
    }
    onSave(result);
    onClose();
  }

  const usedTargets = new Set(mappings.map(m => m.target).filter(Boolean));

  return (
    <Dialog
      className="clone-server-mapping-dialog"
      title="Clone server mappings"
      isOpen
      onClose={onClose}
      style={{ width: 600 }}
    >
      {servers ? (
        <>
          <div className={Classes.DIALOG_BODY}>
            <Callout
              intent={Intent.PRIMARY}
              icon={IconNames.INFO_SIGN}
              style={{ marginBottom: 10 }}
            >
              Each target server clones all segments from its source server. The target does not
              participate in regular segment assignment or balancing.
            </Callout>
            {mappings.length > 0 && (
              <table className={Classes.HTML_TABLE} style={{ width: '100%' }}>
                <thead>
                  <tr>
                    <th>Target</th>
                    <th />
                    <th>Source</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {mappings.map((mapping, i) => (
                    <tr key={i}>
                      <td>
                        <ServerSelect
                          servers={servers}
                          value={mapping.target}
                          disabledServers={usedTargets}
                          currentValue={mapping.target}
                          onChange={v => updateMapping(i, 'target', v)}
                        />
                      </td>
                      <td style={{ textAlign: 'center', verticalAlign: 'middle' }}>clones</td>
                      <td>
                        <ServerSelect
                          servers={servers}
                          value={mapping.source}
                          disabledServers={mapping.target ? new Set([mapping.target]) : undefined}
                          onChange={v => updateMapping(i, 'source', v)}
                        />
                      </td>
                      <td>
                        <Button
                          icon={IconNames.CROSS}
                          minimal
                          intent={Intent.DANGER}
                          onClick={() => removeMapping(i)}
                        />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
            <Button
              icon={IconNames.PLUS}
              text="Add mapping"
              minimal
              onClick={addMapping}
              style={{ marginTop: 5 }}
            />
          </div>
          <div className={Classes.DIALOG_FOOTER}>
            <div className={Classes.DIALOG_FOOTER_ACTIONS}>
              <Button text="Cancel" onClick={onClose} />
              <Button text="Save" intent={Intent.PRIMARY} onClick={handleSave} />
            </div>
          </div>
        </>
      ) : (
        <Loader />
      )}
    </Dialog>
  );
});

// --- Internal server select component ---

interface ServerSelectProps {
  servers: TieredServers;
  value: string;
  disabledServers?: Set<string>;
  currentValue?: string;
  onChange(value: string): void;
}

function ServerSelect(props: ServerSelectProps) {
  const { servers, value, disabledServers, currentValue, onChange } = props;

  // Build a flat list of items with tier headers handled in the renderer
  const allItems = servers.allServers;

  return (
    <Select<string>
      items={allItems}
      itemPredicate={(query, item) => item.toLowerCase().includes(query.toLowerCase())}
      itemListRenderer={({ filteredItems, renderItem }) => {
        const elements: React.ReactNode[] = [];
        let lastTier: string | undefined;
        for (let i = 0; i < filteredItems.length; i++) {
          const item = filteredItems[i];
          const tier = servers.serverToTier[item];
          if (tier && tier !== lastTier) {
            elements.push(<MenuDivider key={`tier-${tier}`} title={tier} />);
            lastTier = tier;
          }
          elements.push(renderItem(item, i));
        }
        return <>{elements}</>;
      }}
      itemRenderer={(item, { handleClick, handleFocus, modifiers }) => {
        if (!modifiers.matchesPredicate) return null;
        const disabled = disabledServers
          ? disabledServers.has(item) && item !== currentValue
          : false;
        return (
          <MenuItem
            key={item}
            text={item}
            active={modifiers.active}
            disabled={disabled}
            onClick={handleClick}
            onFocus={handleFocus}
            roleStructure="listoption"
          />
        );
      }}
      onItemSelect={onChange}
      popoverProps={{ minimal: true }}
      filterable
    >
      <Button
        text={value || '(select server)'}
        rightIcon={IconNames.CARET_DOWN}
        fill
        alignText="left"
      />
    </Select>
  );
}
