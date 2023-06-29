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

import { Button, Menu, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import React, { useState } from 'react';

type OpenState = 'open' | 'alt-open';

export interface MoreButtonProps {
  children: React.ReactNode | React.ReactNode[];
  altExtra?: React.ReactNode;
}

export const MoreButton = React.memo(function MoreButton(props: MoreButtonProps) {
  const { children, altExtra } = props;

  const [openState, setOpenState] = useState<OpenState | undefined>();

  let childCount = 0;
  // Sadly React.Children.count does not ignore nulls correctly
  React.Children.forEach(children, child => {
    if (child) childCount++;
  });

  return (
    <Popover2
      className="more-button"
      isOpen={Boolean(openState)}
      content={
        <Menu>
          {children}
          {openState === 'alt-open' && altExtra}
        </Menu>
      }
      position={Position.BOTTOM_LEFT}
      onInteraction={(nextOpenState, e: any) => {
        if (!e) return; // For some reason this function is always called twice once with e and once without
        setOpenState(nextOpenState ? (e.altKey ? 'alt-open' : 'open') : undefined);
      }}
    >
      <Button icon={IconNames.MORE} disabled={!childCount} title="More actions" />
    </Popover2>
  );
});
