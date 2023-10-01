# Docker Compose
O arquivo docker-compose responsável por "levantar" todos os serviços necessários para o projeto está disponível em projeto_imdb_infra/docker-compose.yml e ele contém os seguintes serviços:

* 4 servidores de storage MinIO
* 1 servidor web para interface MinIO com Ngix
* 1 servidor de banco de dados PosgreSQL para os dados do Airflow
* 1 servidor para o Airflow Scheduler
* 1 servidor web para interface do Airflow
* 1 servidor para as rotinas de inicialização do Airflow
* 1 servidor para o Jupyter Lab
* 1 servidor de banco de dados postgreSQL para o Datawarehouse
* 1 servidor para o Metabase
* 1 servidor para o Spark com função de MASTER
* 1 servidor para o Spark com função de HISTORY
* 4 servidor para o Spark com função de WORKER