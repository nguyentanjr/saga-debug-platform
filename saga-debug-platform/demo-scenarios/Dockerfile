FROM eclipse-temurin:17-jre
WORKDIR /app
COPY target/demo-scenarios-*-SNAPSHOT.jar app.jar
EXPOSE 8085
ENTRYPOINT ["java","-jar","/app/app.jar"]
