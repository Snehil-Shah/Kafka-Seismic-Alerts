FROM maven:latest

LABEL org.opencontainers.image.description="Build a Maven Project using exec-maven-plugin"

ADD . /consumers
WORKDIR /consumers
RUN mvn clean install
CMD [ "bash" ]
