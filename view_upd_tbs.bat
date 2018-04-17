@echo off
set scripts= "D:\daguado\sigcatmin\app_simultaneidad"
set venv= "D:\daguado\sigcatmin\venv"
set date= "%1"

call %venv%\Scripts\activate.bat
%venv%\Scripts\python.exe %scripts%\view_upd_tbs.py %date%
call %venv%\Scripts\deactivate.bat
@echo on
