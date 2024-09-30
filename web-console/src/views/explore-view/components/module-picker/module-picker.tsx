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

import { Button, ButtonGroup, Menu, MenuItem, Popover, Position } from '@blueprintjs/core';
import type { IconName } from '@blueprintjs/icons';
import { IconNames } from '@blueprintjs/icons';
import type { JSX } from 'react';
import React from 'react';

import './module-picker.scss';

export interface ModulePickerProps {
  modules: readonly { id: string; icon: IconName; label: string }[];
  selectedModuleId: string | undefined;
  onSelectedModuleIdChange(newSelectedModuleId: string): void;
  moreMenu?: JSX.Element;
}

export const ModulePicker = React.memo(function ModulePicker(props: ModulePickerProps) {
  const { modules, selectedModuleId, onSelectedModuleIdChange, moreMenu } = props;

  const selectedTileManifest = modules.find(module => module.id === selectedModuleId);
  return (
    <ButtonGroup className="module-picker" fill>
      <Popover
        className="picker-button"
        minimal
        fill
        position={Position.BOTTOM_RIGHT}
        content={
          <Menu>
            {modules.map((module, i) => (
              <MenuItem
                key={i}
                icon={module.icon}
                text={module.label}
                onClick={() => onSelectedModuleIdChange(module.id)}
              />
            ))}
          </Menu>
        }
      >
        <Button
          icon={selectedTileManifest ? selectedTileManifest.icon : IconNames.BOX}
          text={selectedTileManifest ? selectedTileManifest.label : 'Select module'}
          fill
          minimal
          rightIcon={IconNames.CARET_DOWN}
        />
      </Popover>
      {moreMenu && (
        <Popover className="more-button" position={Position.BOTTOM_RIGHT} content={moreMenu}>
          <Button minimal icon={IconNames.MORE} />
        </Popover>
      )}
    </ButtonGroup>
  );
});
