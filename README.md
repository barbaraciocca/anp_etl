<img src="https://scontent-gru1-1.xx.fbcdn.net/v/t39.30808-6/216612648_4090165791030684_2680195912454567511_n.png?_nc_cat=101&ccb=1-7&_nc_sid=5f2048&_nc_ohc=no8FmZfJg60AX8aT2QD&_nc_ht=scontent-gru1-1.xx&oh=00_AfDGCxsu_vxkTJb3oo0CAnQEzYhq1_zA77OyRd5CFlw8Bg&oe=6546AFCC" width="40" height="28">![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)![Docker](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white)![Git](https://img.shields.io/badge/GIT-E44C30?style=for-the-badge&logo=git&logoColor=white)![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white)![LibreOffice](https://img.shields.io/badge/LibreOffice-%2318A303?style=for-the-badge&logo=LibreOffice&logoColor=white)

# Raízen Case 
Raízen is one of the leading energy companies in Brazil, and operates in the fields of fuel production and distribution, sugar, and ethanol. The company is renowned for its innovation and leadership in the biofuels sector, promoting sustainability and carbon emissions reduction. Furthermore, Raízen plays a crucial role in the country's energy supply, with a significant presence across the entire fuel value chain, from production to distribution.

- This repository contains a  Raízen case project to develop a data extraction, tranformation and loading processes for accessing the internal pivot caches within the consolidated reports released by the Brazilian government's regulatory agency for oil and fuels, ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis).

- This pipeline is designed to extract and format the underlying data from two specific tables: Sales of oil derivative fuels by UF and product and Sales of diesel by UF and type.



<details>
<summary><strong>Summary</strong></summary>

- [Requirements](#requirements)
- [Documentations](#documentations)
- [Contact](#contact)

</details>


## Requirements

Note: 

 You must have docker-composer installed on your computer.

 To see the desired table, called ```vendas_combustiveis```, you will need to open a database administration tool, e.g. DBeaver.


- Run the project:

--> Write the list of commands below in your terminal:
```
git clone https://github.com/barbaraciocca/anp_etl.git
cd anp_etl
docker-compose up --build
```
--> Open ```localhost:8080``` in your browser. 
    The user and the password are: ```admin```.

--> Open your database administration tool to see the created table.

Create a new connection with Postgress, using these credentials:
```
host: localhost
password: airflow
user: airflow
port: 5432
database: airflow
```


## Data format

The data was formatted in the following format

| Column   | Example       | Type                          |
| :---------- | :--------- | :---------------------------------- |
|Year_Month| 2023-10 |  `Date` |
|UF| São Paulo | `String`|
|Product| Óleo Diesel | `string`|
|Unit| m3 |  `string`  |
|Volume| 38909.48 | `double` |
|Created_at|2023-10-30T14:25:00| `timestamp`|

## Documentations

| Tool   | Status       | Description                           |
| :---------- | :--------- | :---------------------------------- |
|[Airflow](https://airflow.apache.org/docs/) | `In use` | Used as orquestrator |
|[Docker compose](https://docs.docker.com/) | `In use` | Used for creating a docker image |
|[Python](https://docs.python.org/3/) | `In use` | Used as primary language |
|[Git](https://git-scm.com/doc) | `In use` | Used for code versionament |
|[Postgres](https://www.postgresql.org/docs/) | `In use` | Used as database and data visualization |
|[LibreOffice](https://documentation.libreoffice.org/en/english-documentation/) | `In use` | Used to read and convert file |

## Contact 

If you have any questions, feedback, or need assistance with this project, feel free to contact me in Linkedin:

[![LinkedIn Badge](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/b%C3%A1rbara-etruri-ciocca-79264966/)

Or send me a email:

[![Hotmail](https://img.shields.io/badge/-Hotmail-0078D4?style=flat-square&logo=microsoft-outlook&logoColor=white)](mailto:barbara_ciocca@hotmail.com)
