CREATE TABLE IF NOT EXISTS queue (
    id int PRIMARY KEY,
    is_done boolean
);

INSERT INTO queue SELECT geneate_series(1,3);
