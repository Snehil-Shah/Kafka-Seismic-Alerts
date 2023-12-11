FROM maven:latest
WORKDIR /app
COPY . /app
RUN mvn clean install