# Use Python 3.11 as base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Configure Streamlit
RUN mkdir -p .streamlit
COPY .streamlit/config.toml .streamlit/config.toml

# Expose port
EXPOSE 5000

# Run the application
CMD ["streamlit", "run", "app.py"]
