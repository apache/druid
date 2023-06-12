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

import type { ToasterInstance } from '@blueprintjs/core';
import { OverlayToaster, Position } from '@blueprintjs/core';

let toaster: ToasterInstance | undefined;

export const AppToaster: { show: ToasterInstance['show'] } = {
  show: (...args) => {
    if (!toaster) {
      // Using lazy initialization avoids the following deprecation notice for "ReactDOM.render",
      // both on initial load and in every unit test:
      //
      // Warning: ReactDOM.render is no longer supported in React 18. Use createRoot instead. Until
      // you switch to the new API, your app will behave as if it's running React 17.
      // Learn more: https://reactjs.org/link/switch-to-createroot
      toaster = OverlayToaster.create({ className: 'recipe-toaster', position: Position.TOP });
    }
    return toaster.show(...args);
  },
};
