FROM maven:latest
ADD . /service
WORKDIR /service
RUN mvn clean install
CMD [ "mvn","-q","exec:java" ]
