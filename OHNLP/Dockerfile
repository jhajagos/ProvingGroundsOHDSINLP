FROM ubuntu:focal
RUN apt-get -y update

RUN apt-get -y install software-properties-common

RUN apt-get -y install vim
RUN apt-get -y install wget
RUN apt-get -y install dos2unix

RUN apt-get -y update

# Get JDK 11
RUN apt -y install openjdk-11-jre-headless

RUN apt-get -y install unzip
RUN apt-get -y install less

# Get Backbone release
RUN mkdir -p OHNLP/backbone
RUN cd OHNLP/backbone;  wget https://github.com/OHNLP/Backbone/releases/download/v1.0.13/Backbone.zip
RUN cd OHNLP/backbone;  unzip Backbone.zip

# Get MedTagger release
RUN mkdir  OHNLP/medtagger
RUN cd OHNLP/medtagger/; wget https://github.com/OHNLP/MedTagger/releases/download/v1.0.29/MedTagger.zip
RUN cd OHNLP/medtagger/; unzip MedTagger.zip
RUN cd OHNLP/medtagger/; cp  MedTagger.jar ../backbone/modules/
RUN cd OHNLP/medtagger/; cp -r medtaggerieresources/pasc/ ../backbone/resources/

# Copy configuration file
ARG source="./config/[[]run_nlp]_[[]ohdsi_cdm]_[[]ohdsi_cdm].json"
COPY ${source} OHNLP/backbone/configs/

RUN cd OHNLP/backbone; bash package_modules_and_configs.sh

EXPOSE 8081

# Flink Cluster Started - Job Progress Can be Seen via Configured WebUI Port (Default: localhost:8081)