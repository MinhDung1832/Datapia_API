# 
FROM python:3.9-slim
# install FreeTDS and dependencies
# RUN apt-get update \
#  && apt-get install unixodbc -y \
#  && apt-get install unixodbc-dev -y \
#  && apt-get install freetds-dev -y \
#  && apt-get install freetds-bin -y \
#  && apt-get install tdsodbc -y \
#  && apt-get install --reinstall build-essential -y

# Install dependencies
# Install dependencies
RUN apt-get update && apt-get install -y curl gnupg2 apt-transport-https

# Add the Microsoft package repository for SQL Server ODBC driver
RUN curl -s https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl -s https://packages.microsoft.com/config/debian/10/prod.list | tee /etc/apt/sources.list.d/mssql-release.list

# Install the ODBC Driver 18 for SQL Server and dependencies
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev

# Clean up APT cache to reduce image size
RUN apt-get clean && rm -rf /var/lib/apt/lists/*
# 
WORKDIR /code

# 
COPY ./requirements.txt /code/requirements.txt

# 
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# 
COPY . /code/app

# 
# CMD ["fastapi", "run", "app/main.py", "--port", "210193"]