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

import ace from 'ace-builds';

// Ace does not ship typings for the classes that modes are built from
export const TextMode: any = ace.require('ace/mode/text').Mode;
export const TextHighlightRules: any = ace.require(
  'ace/mode/text_highlight_rules',
).TextHighlightRules;

const defineModule: (name: string, deps: string[], payload: object) => void = (ace as any).define;
const modeCache: Record<string, unknown> = (ace.config as any).$modes;

/**
 * Registers a mode so that editors can refer to it by name, as in `mode="dsql"`. The mode is only registered once
 * (Ace ignores later definitions of a module) so a mode that needs to change should read its configuration when it is
 * constructed and be reset with `resetAceMode`.
 */
export function registerAceMode(name: string, Mode: new () => unknown): void {
  defineModule(`ace/mode/${name}`, [], { Mode });
}

/**
 * Ace creates a single instance of each mode and shares it between all editors, forget it so that editors created from
 * now on get a new instance.
 */
export function resetAceMode(name: string): void {
  delete modeCache[`ace/mode/${name}`];
}
