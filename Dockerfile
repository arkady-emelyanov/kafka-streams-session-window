FROM maven:3.6-slim as builder

WORKDIR /app
# download and cache all dependencies for build
# never fail is used to skip possible test failures
COPY pom.xml .
RUN mvn verify clean --fail-never

# copy the application, re-run tests and package
# final uber-jar file.
COPY . .
RUN mvn package


FROM azul/zulu-openjdk:latest

COPY --from=builder /app/target/kafka-streams-session-jar-with-dependencies.jar /kafka-streams-session.jar
CMD [ "java", "-jar", "/kafka-streams-session.jar"]
