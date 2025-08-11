# Obtención de la data
- La data se obtiene de la web de PLABACOM del coordinador electrico nacional (CEN), el link al momento de escribir este README es: <https://plabacom.coordinador.cl/pages>
- Se debe descargar las versiones **Definitivo** de las siguientes carpetas: Energía, Potencia y SSCC.
- Todas deben ser del mismo mes, el código no aceptará carpetas con fechas distintas.
- Esta descarga son carpetas comprimidas en .zip, **NO SE DEBEN DESCOMPRIMIR**, el código se encargará de esto.
***
# Carga de data
- Muchas de las funcionalidad del código usan expresiones regulares, por lo que un cambio en el formato de nombre afectará inmediatamente, en ese caso reportar para poder actualizar el código.
- Se debe cargar los archivos comprimidos a una carpeta de nombre "zip", no cambiar ningún nombre de las carpetas.
- Las carpetas a cargar deberían ser:
  -  Energia Resultados
  -  Energía Antecedentes de Cálculo
  -  Potencia Balance Psuf
  -  SSCC Balance SSCC
***
# Entorno Virtual

Se debe crear un entorno virtual, este paso es totalmente opcional pero recomendable, para esto se dispuso de dos archivos .bat:
  - entorno.bat : Crea el entorno virtual e instala las dependencias, este solo se ejecuta si el entorno no existe, no es necesario correrlo todas las veces.
  - ejecucion.bat : Activa el entorno virtual y corre el código main.py.

Para ejecutar estos archivos se debe hacer lo siguiente por consola

```bash
#!/bin/bash
./entorno.bat
./ejecucion.bat
```

# Dependencias
- Tener instalado python, en este caso se usó Python 3.13.1
- En este caso se usó Windows 11 para correr el script, por lo que los bat no funcionarán en Linux.
