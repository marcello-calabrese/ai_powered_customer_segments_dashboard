
--make the dimension tables ids foreign keys in fact_hotel table

ALTER TABLE fact_hotel
ADD CONSTRAINT fk_fact_hotel_dim_segment
FOREIGN KEY (segment_id)
REFERENCES dim_segment (segment_id);

-- Add new column to fact_hotel table
ALTER TABLE fact_hotel
ADD COLUMN relation_id SERIAL;

-- Add new column to cluster table
ALTER TABLE cluster
ADD COLUMN relation_id SERIAL;

-- Drop the existing primary key constraint
ALTER TABLE cluster
DROP CONSTRAINT cluster_pkey;

-- Add primary key constraint to relation_id column
ALTER TABLE cluster
ADD PRIMARY KEY (relation_id);

-- Add foreign key constraint to fact_hotel table
ALTER TABLE fact_hotel
ADD CONSTRAINT fk_fact_hotel_cluster
FOREIGN KEY (relation_id)
REFERENCES cluster (relation_id);