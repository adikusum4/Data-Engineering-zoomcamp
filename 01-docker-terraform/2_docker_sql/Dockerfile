FROM python:3.12.1

# Update package manager dan install wget
RUN apt-get update && apt-get install -y wget

# Install Python dependencies
RUN pip install pandas sqlalchemy psycopg2

# Set working directory
WORKDIR /app

# Copy script ingest_data.py ke dalam container
COPY ingest_data.py ingest_data.py 

# Set entrypoint untuk menjalankan script Python
ENTRYPOINT ["python", "ingest_data.py"]
