FROM eclipse-temurin:17-jre
WORKDIR /app
COPY target/correlation-service-*-SNAPSHOT.jar app.jar
EXPOSE 8084
ENTRYPOINT ["java","-jar","/app/app.jar"]
