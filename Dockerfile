# Use an official Python image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy requirement file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . .

# Create required folders
RUN mkdir -p data/staging data/mart

# Set entry point to run the ETL script
CMD ["python", "etl_pipeline.py"]
