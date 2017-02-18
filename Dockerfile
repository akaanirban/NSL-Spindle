FROM wkronmiller/scala-sbt

ENV workdir /opt
ENV vehicledir $workdir/Vehicle
ENV simdir $vehicledir/autoSim
ENV SBT_OPTS "-Xmx90G -XX:+UseConcMarkSweepGC"

# Install NodeJs
RUN apt-get install -y sudo
RUN curl -sL https://deb.nodesource.com/setup_7.x | bash -
RUN sudo apt-get install -y nodejs
RUN npm install -g yarn

COPY . $workdir/

RUN cd $vehicledir && sbt update && sbt compile && cd -

RUN cd $simdir && yarn

WORKDIR $simdir

CMD ["yarn", "start"]
