FROM python:3.12.7-bullseye

WORKDIR /app

# Copy just requirements to build dependencies 
COPY requirements.txt .

# Install the dependencies
RUN pip install -U pip wheel && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Run the API
CMD exec fastapi run api --port ${PORT:-8000}
