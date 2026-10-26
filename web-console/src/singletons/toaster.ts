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

import type { Toaster } from '@blueprintjs/core';
import { OverlayToaster, Position } from '@blueprintjs/core';

let toasterPromise: Promise<Toaster> | undefined;

export const AppToaster = {
  show(...args: Parameters<Toaster['show']>): void {
    // The toaster is created on first use so that pages and unit tests that never show a toast
    // don't create its React root
    toasterPromise ??= OverlayToaster.create({
      className: 'recipe-toaster',
      position: Position.TOP,
    });
    void toasterPromise.then(toaster => toaster.show(...args));
  },
};
