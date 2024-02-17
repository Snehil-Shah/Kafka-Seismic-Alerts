# Build React app
FROM node:21 as build
WORKDIR /app
COPY Web/ ./
RUN npm install
RUN npm run build

# Initialize Server
FROM python:latest
LABEL org.opencontainers.image.description="Run multiple indefinite .py scripts simultaneously. Scripts: RestAPI.py & WebSocket.py"

WORKDIR /service
COPY --from=build /app/dist ./Web/build
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 5000
CMD python RestAPI.py & python WebSocket.py