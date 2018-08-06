@ECHO OFF

REM Redirect the user to use the bash script under CygWin environment
REM otherwise CTRL-C leaves subprocesses running.
IF DEFINED SHELL (
  IF DEFINED ORIGINAL_PATH (
    ECHO Use bash script on CygWin instead of its .cmd Windows equivalent
    EXIT /b 1
  )
)

SETLOCAL

REM Determine installation home
SET SCRIPT_DIR=%~dp0

REM Launch
java -jar "%SCRIPT_DIR%\lib\ramdir2-${project.version}.jar"  %*
SET EXITVAL=%errorlevel%
exit /b %EXITVAL%
