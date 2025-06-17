# Imagen base de Python
FROM python:3.10-slim

# Establecer directorio de trabajo
WORKDIR /app

# Copiar solo los archivos necesarios
COPY app.py .
COPY requirements.txt .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar .env y credenciales solo si los montas desde fuera (opcional)
# NO se incluyen en la imagen

# Comando por defecto al iniciar el contenedor
CMD ["python", "app.py"]
