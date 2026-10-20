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

import { Button, Classes, Dialog, FormGroup, Intent, NumericInput } from '@blueprintjs/core';
import React, { useState } from 'react';

interface PageJumpDialogProps {
  initPage: number;
  maxPage: number;
  onJump(page: number): void;
  onClose(): void;
}

export const PageJumpDialog = React.memo(function PageJumpDialog(props: PageJumpDialogProps) {
  const { initPage, maxPage, onJump, onClose } = props;

  const [page, setPage] = useState<string>(String(initPage + 1));
  const pageAsNumber = parseInt(page, 10);
  const validPage = pageAsNumber >= 1;

  return (
    <Dialog
      className="page-jump-dialog"
      onClose={onClose}
      isOpen
      title="Jump to page"
      canOutsideClickClose={false}
    >
      <div className={Classes.DIALOG_BODY}>
        <FormGroup>
          <NumericInput
            value={page}
            onValueChange={(_vn, vs) => setPage(vs)}
            autoFocus
            selectAllOnFocus
            min={1}
            max={maxPage}
            minorStepSize={1}
            fill
          />
        </FormGroup>
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="Go"
            intent={Intent.PRIMARY}
            disabled={!validPage}
            onClick={() => {
              onJump(pageAsNumber - 1);
              onClose();
            }}
          />
        </div>
      </div>
    </Dialog>
  );
});
