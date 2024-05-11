# Build React app
FROM node:21 as build
WORKDIR /app
COPY web/ ./
RUN npm install
RUN npm run build

# Initialize Server
FROM python:latest
LABEL org.opencontainers.image.description="Run multiple indefinite .py scripts simultaneously. Scripts: RestAPI.py & WebSocket.py"

WORKDIR /service
COPY --from=build /app/dist ./web/build
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 5000
CMD python integrations/RestAPI.py & python integrations/WebSocket.py