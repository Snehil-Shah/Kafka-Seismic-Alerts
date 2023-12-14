FROM python:latest

LABEL org.opencontainers.image.description="Run multiple indefinite .py scripts simultaneously. Scripts: RestAPI.py & WebSocket.py"

WORKDIR /service
ADD . /service
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 5000
CMD python RestAPI.py & python WebSocket.py