FROM bde2020/hadoop-base
COPY target/Parquet-1.0-SNAPSHOT.jar /
CMD [ "hadoop", "jar", "/Parquet-1.0-SNAPSHOT.jar", "ToParquet" ]