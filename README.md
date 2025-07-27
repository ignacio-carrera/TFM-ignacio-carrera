Este README describe los pasos necesarios para conectarse a la API REST de OpenAir.

Para integrar una aplicación con la API, es necesario crear una aplicación en OpenAir. Una vez creada, la aplicación proporciona dos tipos de tokens:

Access Token, con una duración de 15 minutos, utilizado para autenticar las solicitudes a la API.

Refresh Token, con una duración de 1 día, que permite obtener nuevos Access Tokens sin intervención del usuario.

Si el Refresh Token expira (después de 24 horas sin renovación) o se revoca, será necesario volver a autenticar y autorizar la aplicación, tal como se indica en el paso llamado "Refrescar el token Oauth2".

# Manual de uso:

### Creación un entorno virtual:

Creamos un entorno virtual de Python llamado .venv en el directorio actual:
```bash
python3 -m venv .venv
```
### Activación del entorno virtual

```
source .venv/bin/activate
```
### Instalación de las dependencias:

Este comando instalará todas las bibliotecas listadas en el archivo requirements.txt:
```
pip install -r requirements.txt
```

### Ejecutamos la app_finance:

Ejecutamos el siguiente script para correr el ETL de users y billable_hours, además de poder visualizar los dashboards de métricas.
```
streamlit run app_finance.py
```
### Ejecutamos la app_employees:

Ejecutamos el siguiente script para abrir el módulo de empleados y asi poder cargar y validar las facturas.
```
streamlit run app_employees.py
```
### Refrescar el token OAuth2:

Cuando el token expira, ejecutamos el siguiente script dentro de la carpeta access_openair. En el navegador, selecciona la cuenta correspondiente a 7050007 Blend360 LLC para refrescar el token:
```
python oauth2_openair.py
```

### Para correr tests pytests:
Parados en main ejecutamos:
```
PYTHONPATH=. pytest tests/
```


