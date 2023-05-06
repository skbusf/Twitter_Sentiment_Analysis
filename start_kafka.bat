@echo off

set folder=C:\tmp
if not exist %folder% (
    echo The folder does not exist.
    exit /b 1
)
for /d %%i in ("%folder%\*") do (
    echo Deleting %%i...
    rmdir /s /q "%%i"
)

setlocal enableDelayedExpansion

cd C:\kafka_2.13-3.4.0\bin\windows

start "" cmd /c call .\zookeeper-server-start.bat ..\..\config\zookeeper.properties

:: Wait for ZooKeeper to start
:loop1
set port=2181
for /f "tokens=*" %%a in ('netstat -an ^| findstr /C:":!port! "') do set running=true
if not defined running (
    timeout /t 1 >nul
    goto loop1
)

timeout /t 5

start cmd.exe /k "cd C:\kafka_2.13-3.4.0\bin\windows && .\kafka-server-start.bat ..\..\config\server.properties"

:: Wait for Kafka to start
:loop2
set port=9092
for /f "tokens=*" %%a in ('netstat -an ^| findstr /C:":!port! "') do set running=true
if not defined running (
    timeout /t 1 >nul
    goto loop2
)

echo Kafka broker service is running on port 9092.

