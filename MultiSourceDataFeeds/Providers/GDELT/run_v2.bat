@echo off
set conda_python_env="%LOCALAPPDATA%\ESRI\conda\envs\user-msdf\python.exe"
set conda_activate="%PROGRAMFILES%\ArcGIS\Pro\bin\Python\Scripts\activate.bat"
call %conda_activate% user-msdf
call %conda_python_env% "%~dp0v2_runner.py"