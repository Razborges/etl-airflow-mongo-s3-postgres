##### etl-airflow-mongo-s3-postgres

# Desafio Final do Bootcamp IGTI de Engenharia de Dados

## O que este projeto faz

Este projeto se conecta com uma base de dados MongoDB e em uma api pública do IBGE, salva as informações de ambos em um bucket S3 simulando um DataLake e em paralelo salva os dados em um banco de dados Postgres simulando um DW.

Este projeto foi desenvolvido para o Desafio Final do Bootcamp de Engenharia de Dados do IGTI, utilizando as seguintes tecnologias:

- Apache Airflow 2
- Pandas
- Pymongo
- SqlAlchemy
- Docker

</br>
</br>
</br>

## Executando o projeto:

Para executar este projeto é necessário que **tenha o Docker instalado** em sua máquina.

</br>

1 - Na raiz do projeto, no terminal, utilize os comandos abaixo para rodar para rodar:

Versão local:
```
    docker-compose -f docker-compose-local.yml up -d
```
Versão de produção:

```
    docker-compose -f docker-compose-production.yml up -d
```

</br>

2 - Para validar a subida, acesse em seu navegador o endereço ```localhost:8080``` para abrir o painel do Airflow.

</br>

3 - Para que todo o processo de ETL funcione corretamente ainda é necessário fazer dois passos.

O primeiro é configurar um bucket s3 local, utilize a lib ```awslocal``` para facilitar o processo.

Em seu terminal rode o comando abaixo para instalar a lib:

```pip install awscli-local```

Para criar o bucket:

```awslocal s3 mb s3://airflow```

Para validar que o bucket foi criado:

```awslocal s3 ls```

Ou acesse ```localhost:4566```

</br>

4 - O segundo é criar as variáveis de ambiente diretamente no airflow, para isso utilize o arquivo _variables_example.json_, preencha os dados necessário e o importe no painel do airflow para alimentar todas as variáveis do projeto.

Agora basta ligar e executar a DAG.

</br>
</br>
</br>

## Para configurar o AirFlow

Nessa implementação foi utilizado a imagem https://github.com/puckel/docker-airflow neste repositório existe toda a documentação com os detalhes para as diversas configurações.

## Libs Python utilizadas:

- boto3
- pandas
- pymongo
- loggin
- sqlalchemy
- datetime
- requests
- json
- os