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

import { Button, ButtonGroup, Menu, MenuItem, PopoverNext } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import { ModuleRepository } from '../../module-repository/module-repository';

import './module-picker.scss';

export interface ModulePickerProps {
  selectedModuleId: string | undefined;
  onSelectedModuleIdChange(newSelectedModuleId: string): void;
  fill?: boolean;
}

export const ModulePicker = React.memo(function ModulePicker(props: ModulePickerProps) {
  const { selectedModuleId, onSelectedModuleIdChange, fill } = props;

  const modules = ModuleRepository.getAllModuleEntries();
  const selectedModule = selectedModuleId
    ? ModuleRepository.getModule(selectedModuleId)
    : undefined;

  return (
    <ButtonGroup className="module-picker" fill={fill}>
      <PopoverNext
        className="picker-button"
        animation="minimal"
        arrow={false}
        fill={fill}
        placement="bottom-start"
        content={
          <Menu>
            {modules.map((module, i) => (
              <MenuItem
                key={i}
                icon={module.icon}
                text={module.title}
                onClick={() => onSelectedModuleIdChange(module.id)}
              />
            ))}
          </Menu>
        }
        lazy
        shouldReturnFocusOnClose={false}
      >
        <Button
          icon={selectedModule ? selectedModule.icon : IconNames.BOX}
          text={selectedModule ? selectedModule.title : 'Select module'}
          fill={fill}
          variant="minimal"
          endIcon={IconNames.CARET_DOWN}
        />
      </PopoverNext>
    </ButtonGroup>
  );
});
