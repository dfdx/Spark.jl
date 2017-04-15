@echo off
REM Add extra line like in sh script
echo "In Pipetest"

REM Code lifted from http://stackoverfilow.com/a/6980605
REM extra teps required to cope with lines starting with ;
REM and blank lines which don't work with standard for /F
REM syntax.
setlocal DisableDelayedExpansion

for /F "tokens=*" %%a in ('findstr /n $') do (
  set "line=%%a"
  setlocal EnableDelayedExpansion
  set "line=!line:*:=!"
  echo(!line!
  endlocal
)