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

import { Button, ButtonGroup, Menu, MenuItem, Position } from '@blueprintjs/core';
import type { IconName } from '@blueprintjs/icons';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import type { JSX } from 'react';
import React from 'react';

import './tile-picker.scss';

export interface TilePickerProps<Name extends string> {
  modules: readonly { moduleName: Name; icon: IconName; label: string }[];
  selectedTileName: Name | undefined;
  onSelectedTileNameChange(newSelectedTileName: Name): void;
  moreMenu?: JSX.Element;
}

declare function TilePickerComponent<Name extends string>(
  props: TilePickerProps<Name>,
): JSX.Element;

export const TilePicker = React.memo(function TilePicker(props: TilePickerProps<string>) {
  const { modules, selectedTileName, onSelectedTileNameChange, moreMenu } = props;

  const selectedTileManifest = modules.find(module => module.moduleName === selectedTileName);
  return (
    <ButtonGroup className="tile-picker" fill>
      <Popover2
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
                onClick={() => onSelectedTileNameChange(module.moduleName)}
              />
            ))}
          </Menu>
        }
      >
        <Button
          icon={selectedTileManifest ? selectedTileManifest.icon : IconNames.BOX}
          text={selectedTileManifest ? selectedTileManifest.label : 'Please select something'}
          fill
          minimal
          rightIcon={IconNames.CARET_DOWN}
        />
      </Popover2>
      {moreMenu && (
        <Popover2 className="more-button" position={Position.BOTTOM_RIGHT} content={moreMenu}>
          <Button minimal icon={IconNames.MORE} />
        </Popover2>
      )}
    </ButtonGroup>
  );
}) as typeof TilePickerComponent;
