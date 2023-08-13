-- This query retrieves data from the 'transactions_s3' and 'images_s3' tables and performs a LEFT JOIN.

SELECT 
    i_s.S3_URL AS S3_URL,
    cd.*
FROM 
    transactions_s3 as cd
LEFT JOIN
    images_s3 AS i_s
    ON i_s.ARTICLE_ID = cd.ARTICLE_ID;
