##

```bash
cd ./OHNLP/config

cp '[run_nlp]_[ohdsi_cdm]_[ohdsi_cdm].example.json' '[run_nlp]_[ohdsi_cdm]_[ohdsi_cdm].json'
vim '[run_nlp]_[ohdsi_cdm]_[ohdsi_cdm].json' # edit local

docker build -t ohnlp_backbone:latest ./
docker run -it ohnlp_backbone:latest /bin/bash

```