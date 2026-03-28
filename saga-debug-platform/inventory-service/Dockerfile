FROM eclipse-temurin:17-jre
WORKDIR /app
COPY target/inventory-service-*-SNAPSHOT.jar app.jar
EXPOSE 8083
ENTRYPOINT ["java","-jar","/app/app.jar"]