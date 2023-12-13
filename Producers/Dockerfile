FROM python:latest
WORKDIR /service
ADD . /service
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 5000
CMD python RestAPI.py & python WebSocket.py