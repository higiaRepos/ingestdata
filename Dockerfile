# Usa una imagen base de Python
FROM python:3.9-slim

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Instalar las dependencias del sistema para psycopg2 (PostgreSQL)
RUN apt-get update && apt-get install -y libpq-dev

# Crear la carpeta tmps
RUN mkdir -p /app/tmps

# Copiar los archivos necesarios al contenedor (por ejemplo, tu código)
COPY . /app

# Instalar dependencias
RUN pip install --upgrade pip
RUN pip install -r requirements.txt 
# Instalar python-dotenv para cargar variables de entorno desde .env
RUN pip install python-dotenv

# Cargar las variables de entorno al iniciar el contenedor
ENV DAGSTER_HOME="/home/juancarlos/Escritorio/higia/repos/vhir/api/tmps"
ENV SECRET_KEY=super_secret_key

# Exponer el puerto 4000 (donde corre el servicio de Dagster)
EXPOSE 4000

# Comando para ejecutar el servicio
CMD ["dagster", "dev", "-f", "main.py", "-h", "0.0.0.0", "-p", "4000"]

