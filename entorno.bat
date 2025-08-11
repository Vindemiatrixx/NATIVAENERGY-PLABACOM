@echo off

set VENV_DIR=venv

cd /d "%~dp0"

IF NOT EXIST "%VENV_DIR%" (
    echo Creando entorno virtual.
    python -m venv "%VENV_DIR%"
)

call "%VENV_DIR%\Scripts\activate.bat"


IF EXIST requirements.txt (
    ECHO Leyendo requirements.txt para instalar las dependencias.
    pip install --upgrade pip
    pip install -r requirements.txt
) ELSE (

    ECHO No existe el archivo requirements.txt
)

ECHO Programa listo, activar entorno virtual para ejecutar el script de python.

pause