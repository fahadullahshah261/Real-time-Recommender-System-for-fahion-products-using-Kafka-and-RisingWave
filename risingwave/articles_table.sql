
CREATE TABLE articles_t AS
SELECT
  CAST(article_id AS INT),
  CAST(product_code AS INT),
  CAST(product_type_no AS INT),
  product_group_name,
  CAST(graphical_appearance_no AS INT),
  CAST(colour_group_code AS INT),
  CAST(perceived_colour_value_id AS INT),
  CAST(perceived_colour_master_id AS INT),
  CAST(department_no AS INT),
  index_code,
  CAST(index_group_no AS INT),
  CAST(section_no AS INT),
  CAST(garment_group_no AS INT)
FROM articles;
