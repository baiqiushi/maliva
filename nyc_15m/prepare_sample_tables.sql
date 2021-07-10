-- 1% sample table
CREATE TABLE nyc_150k AS SELECT * FROM nyc_15m TABLESAMPLE BERNOULLI(1);
-- create btree on pickup_datetime
CREATE INDEX idx_nyc_150k_pickup_datetime ON nyc_150k USING BTREE (pickup_datetime);
-- create btree on trip_distance
CREATE INDEX idx_nyc_150k_trip_distance ON nyc_150k USING BTREE (trip_distance);
-- create rtree on pickup_coordinates
CREATE INDEX idx_nyc_150k_pickup_coordinates ON nyc_150k USING RTREE (pickup_coordinates);


-- 4% sample table
CREATE TABLE nyc_600k AS SELECT * FROM nyc_15m TABLESAMPLE BERNOULLI(4);
-- create btree on pickup_datetime
CREATE INDEX idx_nyc_600k_pickup_datetime ON nyc_600k USING BTREE (pickup_datetime);
-- create btree on trip_distance
CREATE INDEX idx_nyc_600k_trip_distance ON nyc_600k USING BTREE (trip_distance);
-- create rtree on pickup_coordinates
CREATE INDEX idx_nyc_600k_pickup_coordinates ON nyc_600k USING RTREE (pickup_coordinates);

