
CREATE TABLE images_t AS
SELECT 
    cast(article_id AS INT), 
    'https://h-and-m-kaggle-images.s3.us-west-2.amazonaws.com/' || article_id || '.jpg' AS S3_URL
FROM 
    images;
ORDER BY articles_id;
