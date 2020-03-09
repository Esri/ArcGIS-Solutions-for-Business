:: Change to current directory of batch file
CD /d %~dp0

:: Read in config.ini file following variables extract variables
::	- client_id
::	- client_secret
::	- web_layer
::  - username
::  - password
for /f "delims=" %%a in (config.ini) do set %%a

:: Root directory path of GP tool folder
:: for %%a in ("%~dp0..") do set "root_dir=%%~fa"

:: Set Defaults environment variables
set python_env="C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"
set datapull_script="%CD%\DataminrMultidayPull.py"
set append_script="%CD%\UpdateGoldData.py"

:: Default variables for Data Pull Script
set query_type=List
set list_selection="Top Events + Alerts : topics-all"
set keyword_query=""
set time_val=20
set time_unit=Minutes
set output_fc_name=alert_query
set output_fgdb="%CD%\BatchUpdate.gdb"

:: Default variables for append script
set query_layer="%CD%\BatchUpdate.gdb\alert_query"

%python_env% %datapull_script% %query_type% %list_selection% %keyword_query% %time_val% %time_unit% %output_fc_name% %output_fgdb%

echo "Finished pulling data from Dataminr"

%python_env% %append_script% %query_layer% %item_id% %org_url% %user_name% %password%

echo "Finished appending data"