# Réponses du test

## _Utilisation de la solution (étape 1 à 3)_

1. La solution utilise docker compose pour déployer plusieurs conteneurs pour airflow, ainsi que la solution, qui inclue l'API REST et l'etl pyspark
 voir le ficher 'run_compose.sh' pour les étapes d'execution

 2. e projet utilise Docker Compose pour déployer plusieurs services, y compris :

Apache Airflow : Pour orchestrer les workflows ETL.
FastAPI : exposé sur localhost:8000.
PySpark: pour développer l'ETL

A noter que la version de delta somble ne pas fonctionner sur docker (pas le temps pour fixer ça), ainsi le code qui permet de gérer l'initial load et l'incremental load ne fonctionne pas sur le conteneur (incluant ses tests unitaires)

3. les tests unitaires pour les 2 étapes du process:
 1- la class Readers, qui implémente la fonction de lecture à partir d'API sur un dataframe
 2- la class Loaders, qui implémente la fonction de chargement (initial load et incremental load) sur le lakehouse

## Questions (étapes 4 à 7)

### Étape 4

Le données sont chargées sur un lakehouse avec un SCD type 2 pour gérer l'historique:
 - open-format: parquet
 - portabilité: standalone sur docker, S3, ADLS, Databricks, ...
 - transactions ACID
 - permet de gérer à la fois les uses cases DS et BI
 - Timetravel: permet de gérer toutes versions et faire des rolback si besoin
 - préparation des datasets pour les DS facilement; il suffit de lire les 3 sources et faire un petit code de transformations en spark pour préparer la zone gold avec des jointures des 3 sources


### Étape 5

l'utilisation de Airflow, permet à la fois d'orchestrer les pipelines, mais aussi de récupérer des métriques qui permetent de surveiller la bonne execution de nos pipeline:
- DAG Metrics:
 -- DAG Run Duration
 -- DAG Run Success Rate
- Task Metrics:
 -- Task Duration
 -- Task Success Rate
 -- Task Failures ..etc
Il est aussi possible d'utiliser un grafana pour monter un dashboard de santé des pipelines

### Étape 6 et 7

mettre en place un worflow MLOps qui permet d'automatiser le cycle de vie du modèle de recommandations.
Le modèle se connecter sur la zone gold pour récupérer les datasets préparés en amont, et génère les recommandations qui doivent être stockées
On peut utiliser une platforme ML comme Azure ML pour gérer le cycle de vie complet: création ou chargement des modèle, expérimentations, déploiement du modèle, exposition du modèle et surveillence du modèle
Il est possible de mettre un workflow qui permet de réentrainer automatiquement le modèle en se basant sur les metrics surveillées
