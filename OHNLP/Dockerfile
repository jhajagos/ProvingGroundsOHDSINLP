FROM ubuntu:focal
RUN apt-get -y update

RUN apt-get -y install software-properties-common

RUN apt-get -y install vim
RUN apt-get -y install wget
RUN apt-get -y install dos2unix


# Get lastest Java
RUN apt-get -y update

RUN apt -y install openjdk-11-jre-headless

RUN apt-get -y install unzip
RUN apt-get -y install less

# Get latest backbone software
RUN mkdir -p OHNLP/backbone
RUN cd OHNLP/backbone;  wget https://github.com/OHNLP/Backbone/releases/download/v1.0.9/Backbone.zip
RUN cd OHNLP/backbone;  unzip Backbone.zip

RUN mkdir  OHNLP/medtagger
RUN cd OHNLP/medtagger/; wget https://github.com/OHNLP/MedTagger/releases/download/v1.0.22/MedTagger.zip
RUN cd OHNLP/medtagger/; unzip MedTagger.zip
RUN cd OHNLP/medtagger/; cp  MedTagger.jar ../backbone/modules/
RUN cd OHNLP/medtagger/; cp -r medtaggerieresources/pasc/ ../backbone/resources/


# Working out SPARC configuration
#RUN cd /opt/
#RUN wget https://archive.apache.org/dist/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz
#RUN tar -xvzf https://archive.apache.org/dist/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz
#
#RUN export SPARK_HOME=/opt/spark-2.4.7-bin-hadoop2.7/
#RUN export PATH=$PATH:$SPARK_HOME/bin/

ARG source="./config/[[]run_nlp]_[[]ohdsi_cdm]_[[]ohdsi_cdm].json"
COPY ${source} OHNLP/backbone/configs/
COPY run_pipeline_local.sh OHNLP/backbone/
RUN cd OHNLP/backbone; bash package_modules_and_configs.sh
