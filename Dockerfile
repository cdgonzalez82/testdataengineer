FROM ubuntu:latest
ENV TZ=Europe/Kiev
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get update && apt-get install -y python3
RUN apt install -y python3-pip
RUN pip install pyspark
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    apt-get install -y ant && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/ && \
    rm -rf /var/cache/oracle-jdk8-installer;
WORKDIR /app
COPY program.py /app/program.py
COPY clean_bookings.json /app/clean_bookings.json
COPY bookings.csv /app/inputs/bookings.csv
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME
ENTRYPOINT ["python3"]
CMD ["program.py"]