# Step 1: build the code with Maven
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY . .
RUN mvn -B -q install -DskipTests -pl relay -am \
 && mvn -B -q -pl relay dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=target/lib

# Step 2: a small image with only Java and our jars
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/relay/target/relay-*.jar app.jar
COPY --from=build /src/relay/target/lib lib
ENTRYPOINT ["java", "-Xmx128m", "-cp", "app.jar:lib/*", "io.github.mekkaouiossamamoussa.orderstream.relay.RelayApp"]
