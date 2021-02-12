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

import { Menu, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

export interface ReorderMenuProps {
  things: any[] | undefined;
  selectedIndex: number;
  moveTo: (newIndex: number) => void;
}

export const ReorderMenu = React.memo(function ReorderMenu(props: ReorderMenuProps) {
  const { things, selectedIndex, moveTo } = props;

  if (!Array.isArray(things) || things.length <= 1 || selectedIndex === -1) return null;
  const lastIndex = things.length - 1;
  return (
    <Menu>
      {selectedIndex > 0 && (
        <MenuItem
          icon={IconNames.DOUBLE_CHEVRON_LEFT}
          text="Make first"
          onClick={() => moveTo(0)}
        />
      )}
      {selectedIndex > 0 && (
        <MenuItem
          icon={IconNames.CHEVRON_LEFT}
          text="Move left (earlier in the sort order)"
          onClick={() => moveTo(selectedIndex - 1)}
        />
      )}
      {selectedIndex < lastIndex && (
        <MenuItem
          icon={IconNames.CHEVRON_RIGHT}
          text="Move right (later in the sort order)"
          onClick={() => moveTo(selectedIndex + 1)}
        />
      )}
      {selectedIndex < lastIndex && (
        <MenuItem
          icon={IconNames.DOUBLE_CHEVRON_RIGHT}
          text="Make last"
          onClick={() => moveTo(lastIndex)}
        />
      )}
    </Menu>
  );
});
