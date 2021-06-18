## Database Integration

- This project will be using SQLite with SQLAlchemy for the database integration. 

- Three datasets that were brought in to the database are the clustered_dataset, the finalized_data and the for_vis_dataset
  - The clustered_dataset is the main one that contains the clusters, VORP and WS from 2003-2017
  - the for_vis_dataset is the clustered_dataset but with VORP and WS from before 2003 added in as wel
  - The finalized_datais the cleaned dataset that was used to create the clustered_dataset after the ETL process
