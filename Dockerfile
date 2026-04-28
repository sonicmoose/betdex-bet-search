FROM gradle:8.7-jdk21 AS build
WORKDIR /workspace
COPY settings.gradle build.gradle ./
COPY app/build.gradle ./app/build.gradle
COPY app/src ./app/src
RUN gradle --no-daemon :app:bootJar -x test

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/app/build/libs/betdex-indexer.jar /app/app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
