ARG JAVA_VERSION=17
FROM amazoncorretto:${JAVA_VERSION} AS dependencies

RUN yum install -y curl

ARG IVY_VERSION=2.5.2
RUN curl -L -o ivy.jar "http://search.maven.org/remotecontent?filepath=org/apache/ivy/ivy/${IVY_VERSION}/ivy-${IVY_VERSION}.jar"

ARG STORAGE_BACKEND=local
ARG HADOOP_VERSION=3.3.4
ENV HADOOP_VERSION=${HADOOP_VERSION}
ADD scripts/generate-dependencies.sh /generate-dependencies.sh
RUN /generate-dependencies.sh ${STORAGE_BACKEND}

RUN java -jar ivy.jar -ivy ivy.xml -retrieve "lib/[artifact]-[revision](-[classifier]).[ext]"


FROM amazoncorretto:${JAVA_VERSION}

RUN yum install -y curl procps tar

ARG SPARK_VERSION=3.5.3
RUN curl -sL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" | tar xz -C /opt \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

COPY --from=dependencies /lib /opt/spark/jars

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV SPARK_NO_DAEMONIZE=false
ENV SPARK_CONF_DIR=$SPARK_HOME/conf

RUN useradd -m -u 1000 -s /usr/sbin/nologin spark
USER spark

CMD ["/opt/spark/sbin/start-history-server.sh"]
