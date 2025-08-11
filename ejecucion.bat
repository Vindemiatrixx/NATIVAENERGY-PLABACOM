@echo off

cd /d "%~dp0"

set VENV_DIR=venv

IF NOT EXIST "%VENV_DIR%" (
    echo No se encontro el entorno virtual, ejecuta programa.bat para poder continuar
    pause
    exit /b 1
)

call "%VENV_DIR%\Scripts\activate.bat"

python main.py

pause