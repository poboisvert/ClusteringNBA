# Clustering on NBA League From 2003 to 2018

## Project Overview

Using unsupervised machine learning to analyze player decline in the NBA based on the player archetype.
This analysis will provide assistance to NBA teams for roster building and player adjustments.

The analysis will aim to provide insight on the following questions:

1. Determining the new player archytpe in the modern day position list.
2. Will adjusting the athletes playstyle help improve performance.

## Datasets

- [Kaggle Player Data](https://www.kaggle.com/drgilermo/nba-players-stats?select=player_data.csv)
  - A data [sample](ETL/Data/sample_data.xlsx) has been drafted.

## Communication Protocols

- **Slack**: Team discussions, questions, suggestisons & resource sharing.
- **Google Meet**: Team meetings, discussions
- **[Trello](https://trello.com/b/bpUG9Aoh/final-project-nba)**: Work organization, scheduling

## Application Workflow

### Virtual Environment

> cd db && python3 -m venv env

> source env/bin/activate

> pip install -r requirements.txt

### FastAPI - Backend

> cd db && python app/main.py

#### MongoDB

> mongo

> show dbs

> use players

> db.nba_collection.find()

### Database Integration

- This project will be using SQLite with SQLAlchemy for the database integration.
- Click [**here**](Database/SQL_Database.ipynb) for details regarding the **Database Integration** for this analysis.
- [ERD Diagram](Database/Resources/NBA_Analysis_ERD.png)

### Machine Learning Model

- Click [**here**](Machine_Learning/README.md) for details regarding the **Machine Learning model** for this analysis.
- Click [**here**](Machine_Learning/NBA_PCA.ipynb) for Machine learning code details.

### Data Visualization & Dashboard

- This project will be using Tableau for visualizations & dashboards.
- Click [**here**](Visualization/NBA_PLAYER_ANALYSIS.pptx) for details on ppt.
- Click [**here**](https://public.tableau.com/app/profile/syed.ali.akbar/viz/NBAAnalysis_16232021658490/VORPbyPlayersandAge) to access Tableau visualizations
