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

import { Ace } from 'ace-builds';

interface EditorState {
  undoManager: Ace.UndoManager;
}

export class AceEditorStateCache {
  static states: Record<string, EditorState> = {};

  static saveState(id: string, editor: Ace.Editor): void {
    const session = editor.getSession();
    const undoManager: any = session.getUndoManager();
    AceEditorStateCache.states[id] = {
      undoManager,
    };
  }

  static applyState(id: string, editor: Ace.Editor): void {
    const state = AceEditorStateCache.states[id];
    if (!state) return;
    const session = editor.getSession();
    session.setUndoManager(state.undoManager);
  }

  static deleteStates(ids: string[]): void {
    for (const id of ids) {
      delete AceEditorStateCache.states[id];
    }
  }
}
