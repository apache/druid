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

 /* Add copy-to-clipboard buttons to code blocks */
 window.addEventListener('load', function() {

  const COPY_BUTTON_TEXT_LABEL = 'Copy'
  const COPY_BUTTON_SUCCESS_TEXT_LABEL = 'Copied'

  function addCopyButtons(codeBlockSelector, copyButton) {
    document.querySelectorAll(codeBlockSelector).forEach(function(codeBlock) {
      codeBlock.parentNode.appendChild(copyButton.cloneNode(true));
    });
  }

  function createCopyButton() {
    const copyButton = document.createElement('button');
    copyButton.classList.add('copyCodeButton');
    copyButton.setAttribute('aria-label', 'Copy code to clipboard');
    copyButton.setAttribute('type', 'button');

    /* SVG is 'clipboard' icon from blueprintjs */
    copyButton.innerHTML =
      '<div class="copyCodeButtonText">' +
        '<svg version="1.1" id="Layer_1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" x="0px" y="0px" width="12" height="12" viewBox="0 0 16 16" enable-background="new 0 0 16 16" xml:space="preserve"> <g id="clipboard_3_"> <g> <path fill-rule="evenodd" clip-rule="evenodd" d="M11,2c0-0.55-0.45-1-1-1h0.22C9.88,0.4,9.24,0,8.5,0S7.12,0.4,6.78,1H7 C6.45,1,6,1.45,6,2v1h5V2z M13,2h-1v2H5V2H4C3.45,2,3,2.45,3,3v12c0,0.55,0.45,1,1,1h9c0.55,0,1-0.45,1-1V3C14,2.45,13.55,2,13,2z "/> </g> </g> </svg>' +
        '<span class="copyCodeButtonTextLabel">' + COPY_BUTTON_TEXT_LABEL + '<span>' +
      '</div>';
    return copyButton;
  }

  addCopyButtons('.hljs', createCopyButton());

  const clipboard = new ClipboardJS('.copyCodeButton', {
    target: function(trigger) {
      return trigger.parentNode.querySelector('code');
    },
  });

  function showCopySuccess(copyButtonTextElement) {
    copyButtonTextElement.textContent = COPY_BUTTON_SUCCESS_TEXT_LABEL;
    setTimeout(function() {
      copyButtonTextElement.textContent = COPY_BUTTON_TEXT_LABEL;
    }, 2000);
  }

  clipboard.on('success', function(event) {
    event.clearSelection();
    const copyButtonTextElement = event.trigger.querySelector('.copyCodeButtonTextLabel');
    showCopySuccess(copyButtonTextElement)
  });
});
