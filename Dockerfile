FROM openjdk:17-alpine
EXPOSE 8080

COPY target/autodiscovery-0.0.1-SNAPSHOT.jar .

CMD ["java", "-jar", "autodiscovery-0.0.1-SNAPSHOT.jar"]