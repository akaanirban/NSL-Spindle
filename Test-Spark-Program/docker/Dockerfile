FROM wkronmiller/scala-sbt

ENV workdir /opt

EXPOSE 9092
EXPOSE 2181

ENV JAR_NAME TestSpark.jar

COPY ./$JAR_NAME $workdir/

COPY entrypoint.sh $workdir/

RUN chmod +x $workdir/entrypoint.sh

WORKDIR $workdir

CMD ["./entrypoint.sh"]
