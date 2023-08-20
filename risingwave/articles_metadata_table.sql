
CREATE table articles_metadata_t AS
SELECT
    i_s.S3_URL AS S3_URL,
    cd.*
FROM
    articles_t as cd
LEFT JOIN
    images_t AS i_s
    ON i_s.ARTICLE_ID = cd.ARTICLE_ID;
