/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
/**
 * This script, when included in a html file, can be used to make collapsible menus
 *
 * Typical usage:
 * <script type="text/javascript" language="JavaScript" src="menu.js"></script>
 */

function getFileName(url){
    var fileName = url.substring(url.lastIndexOf('/')+1);
    return fileName;
}

function init(){
    var url = window .location.pathname;
    var fileName = getFileName(url);

    var menuItemGroup = document.getElementById("menu").children;

    for (i = 0; i < menuItemGroup.length; i++) {
        if("menutitle" === menuItemGroup[i].className){
            continue;
        }
        var menuItem = menuItemGroup[i].children;
        if(menuItem.length>0){
            for (j = 0; j < menuItem.length; j++) {
                if(menuItem[j].firstElementChild != null){
                    var linkItem = menuItem[j].firstElementChild;
                    if('a' === linkItem.localName){
                        var linkFile = getFileName(linkItem.href);
                        if(fileName === linkFile && linkItem.href.lastIndexOf("apidocs/zookeeper-server/index.html")<0){
                            linkItem.className = "selected";
                            linkItem.parentNode.parentNode.className = "selectedmenuitemgroup";
                            var title = document.getElementById(linkItem.parentNode.parentNode.id+"Title");
                            title.className="menutitle selected";
                        }
                    }
                }
            }
        }
    }
}
