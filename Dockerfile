FROM wkronmiller/scala-sbt

ENV workdir /opt
ENV vehicledir $workdir/Vehicle
ENV simdir $vehicledir/autoSim

COPY . $workdir/

RUN cd $vehicledir && sbt update && cd -

# Install NodeJs
RUN apt-get install -y sudo
RUN curl -sL https://deb.nodesource.com/setup_7.x | bash -
RUN sudo apt-get install -y nodejs
RUN npm install -g yarn
RUN cd $simdir && yarn

WORKDIR $workdir
