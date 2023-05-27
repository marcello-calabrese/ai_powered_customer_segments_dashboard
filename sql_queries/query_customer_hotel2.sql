-- Create a relationship for the cluster table and fact_hotel table


-- Drop the existing primary key constraint
ALTER TABLE cluster
DROP COLUMN cluster_id;


-- Add PRIMARY KEY constraint to relation_id column in cluster table
ALTER TABLE cluster
ADD PRIMARY KEY (relation_id);

-- Add foreign key constraint to fact_hotel table
ALTER TABLE fact_hotel
ADD CONSTRAINT fk_fact_hotel_cluster
FOREIGN KEY (relation_id)
REFERENCES cluster (relation_id);
