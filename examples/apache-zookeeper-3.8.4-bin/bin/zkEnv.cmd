@echo off
REM Licensed to the Apache Software Foundation (ASF) under one or more
REM contributor license agreements.  See the NOTICE file distributed with
REM this work for additional information regarding copyright ownership.
REM The ASF licenses this file to You under the Apache License, Version 2.0
REM (the "License"); you may not use this file except in compliance with
REM the License.  You may obtain a copy of the License at
REM
REM     http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.

set ZOOCFGDIR=%~dp0%..\conf
set ZOO_LOG_DIR=%~dp0%..\logs

REM for sanity sake assume Java 1.6
REM see: http://java.sun.com/javase/6/docs/technotes/tools/windows/java.html

REM add the zoocfg dir to classpath
set CLASSPATH=%ZOOCFGDIR%

REM make it work in the release
SET CLASSPATH=%~dp0..\*;%~dp0..\lib\*;%CLASSPATH%

REM make it work for developers
SET CLASSPATH=%~dp0..\build\classes;%~dp0..\build\lib\*;%CLASSPATH%

set ZOOCFG=%ZOOCFGDIR%\zoo.cfg

@REM setup java environment variables

if not defined JAVA_HOME (
  echo Error: JAVA_HOME is not set.
  goto :eof
)

set JAVA_HOME=%JAVA_HOME:"=%

if not exist "%JAVA_HOME%"\bin\java.exe (
  echo Error: JAVA_HOME is incorrectly set: %JAVA_HOME%
  echo Expected to find java.exe here: %JAVA_HOME%\bin\java.exe
  goto :eof
)

REM strip off trailing \ from JAVA_HOME or java does not start
if "%JAVA_HOME:~-1%" EQU "\" set "JAVA_HOME=%JAVA_HOME:~0,-1%"
 
set JAVA="%JAVA_HOME%"\bin\java

