CREATE TABLE IF NOT EXISTS reviews (
  id STRING,
  productid STRING,
  userid STRING,
  profilename STRING,
  helpfullnessnumerator INT,
  helpfullnessdenominator INT,
  score INT,
  time INT,
  summary STRING,
  text STRING) row format delimited fields terminated by ',';
LOAD DATA LOCAL INPATH './data/Reviews.csv' OVERWRITE INTO TABLE reviews;
