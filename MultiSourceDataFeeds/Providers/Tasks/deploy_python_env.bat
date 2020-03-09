@echo off
:: Setting local variables
set python_dir="%PROGRAMFILES%\ArcGIS\Pro\bin\Python\envs\arcgispro-py3"
set conda="%PROGRAMFILES%\ArcGIS\Pro\bin\Python\Scripts\conda.exe"
set conda_activate="%PROGRAMFILES%\ArcGIS\Pro\bin\Python\Scripts\activate.bat"
set conda_deactivate="%PROGRAMFILES%\ArcGIS\Pro\bin\Python\Scripts\deactivate.bat"
set conda_env="%LOCALAPPDATA%\ESRI\conda\envs\user-msdf"
set python_env="%LOCALAPPDATA%\ESRI\conda\envs\user-msdf\python.exe"

:: Check if cloned environment already exists and delete if necessary
IF EXIST %conda_env% ECHO %conda_env% Already exists. Refreshing with a new cloned environment.
call %conda_deactivate%
IF EXIST %conda_env% rmdir /S /Q %conda_env%

echo Creating a new python environment named user-msdf...
:: Adding the -v flag will provide more details on the symlinking of each 
:: base library to the new user environment.
call %conda% create --name user-msdf --clone %python_dir% -y

echo .........................................
echo ... Activating New Python Environment ...
echo .........................................
call %conda_activate% user-msdf
echo Python Environment:
pip -V
call %conda% info --envs

echo .............................................................................
echo ... Installing required packages for the Multi Source Data Feeds Solution ...
echo .............................................................................

pip install newspaper3k --user
call %python_env% -c "import nltk; nltk.download('punkt')"
pip install feedparser --user
pip install pycryptodome --user

echo .............................................
echo ... Finished installing required packages ...
echo .............................................
echo Please restart ArcGIS Pro and activate the new python environment, user-msdf.
timeout /t -1