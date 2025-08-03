-- 01.Identificar e tratar valores nulos
SELECT
  COUNT(*) AS total_nulos,

  COUNTIF(order_id IS NULL) AS nulos_order_id,
  COUNTIF(customer_ID IS NULL) AS nulos_customer_ID,
  COUNTIF(product_id IS NULL) AS nulos_product_id,
  COUNTIF(order_date IS NULL) AS nulos_order_date,
  COUNTIF(ship_date IS NULL) AS nulos_ship_date,
  COUNTIF(ship_mode IS NULL) AS nulos_ship_mode,
  COUNTIF(order_priority IS NULL) AS nulos_order_priority,
  COUNTIF(sales IS NULL) AS nulos_sales,
  COUNTIF(quantity IS NULL) AS nulos_quantity,
  COUNTIF(profit IS NULL) AS nulos_profit,
  COUNTIF(discount IS NULL) AS nulos_discount,
  COUNTIF(shipping_cost IS NULL) AS nulos_shipping_cost

FROM `superstorevendas.base_vendas`

-- 02.Identificar e tratar valores duplicados
SELECT
  row_id,
  order_id,
  order_date,
  ship_date,
  ship_mode,
  customer_ID,
  customer_name,
  segment,
  city,
  state,
  country,
  region,
  market,
  market2,
  product_id,
  category,
  sub_category,
  product_name,
  sales,
  quantity,
  discount,
  profit,
  shipping_cost,
  order_priority,
  COUNT(*) AS total_duplicados
FROM `superstorevendas.base_vendas`
GROUP BY
  row_id,
  order_id,
  order_date,
  ship_date,
  ship_mode,
  customer_ID,
  customer_name,
  segment,
  city,
  state,
  country,
  region,
  market,
  market2,
  product_id,
  category,
  sub_category,
  product_name,
  sales,
  quantity,
  discount,
  profit,
  shipping_cost,
  order_priority
HAVING total_duplicados > 1
ORDER BY total_duplicados DESC;

-- 03. Identificar e tratar dados discrepantes em variáveis ​​categóricas
SELECT DISTINCT TRIM(LOWER(ship_mode)) AS ship_mode_corrigido
FROM `superstorevendas.base_vendas`;

SELECT DISTINCT TRIM(LOWER(order_priority)) AS order_priority_corrigido
FROM `superstorevendas.base_vendas`;

SELECT DISTINCT TRIM(LOWER(segment)) AS segment_corrigido
FROM `superstorevendas.base_vendas`;

SELECT DISTINCT TRIM(LOWER(category)) AS category_corrigido
FROM `superstorevendas.base_vendas`;

SELECT DISTINCT TRIM(LOWER(sub_category)) AS sub_category_corrigido
FROM `superstorevendas.base_vendas`;

SELECT DISTINCT TRIM(LOWER(region)) AS region_corrigido
FROM `superstorevendas.base_vendas`;

SELECT DISTINCT TRIM(LOWER(country)) AS country_corrigido
FROM `superstorevendas.base_vendas`;

SELECT DISTINCT TRIM(LOWER(market)) AS market_corrigido
FROM `superstorevendas.base_vendas`;

-- 04.Identificar e tratar dados discrepantes em variáveis numéricas
SELECT
  MIN(sales) AS min_sales,
  MAX(sales) AS max_sales,
  AVG(sales) AS avg_sales,

  MIN(quantity) AS min_quantity,
  MAX(quantity) AS max_quantity,
  AVG(quantity) AS avg_quantity,

  MIN(discount) AS min_discount,
  MAX(discount) AS max_discount,
  AVG(discount) AS avg_discount,

  MIN(profit) AS min_profit,
  MAX(profit) AS max_profit,
  AVG(profit) AS avg_profit,

  MIN(shipping_cost) AS min_shipping_cost,
  MAX(shipping_cost) AS max_shipping_cost,
  AVG(shipping_cost) AS avg_shipping_cost

FROM `superstorevendas.base_vendas`;

-- 05. Alteração tipo de dado
SELECT
  CAST(order_date AS DATE) AS order_date_corrigido,
  CAST(ship_date AS DATE) AS ship_date_corrigido
FROM `superstorevendas.base_vendas`

-- 06. Criação tabela base limpa
CREATE OR REPLACE TABLE `superstorevendas.base_vendas_limpa` AS
SELECT
  TRIM(LOWER(category)) AS category,
  TRIM(LOWER(city)) AS city,
  TRIM(LOWER(country)) AS country,
  customer_ID,
  TRIM(LOWER(customer_name)) AS customer_name,
  TRIM(LOWER(market)) AS market,
  TRIM(LOWER(order_priority)) AS order_priority,
  TRIM(LOWER(product_name)) AS product_name,
  TRIM(LOWER(region)) AS region,
  TRIM(LOWER(segment)) AS segment,
  TRIM(LOWER(ship_mode)) AS ship_mode,
  TRIM(LOWER(state)) AS state,
  TRIM(LOWER(sub_category)) AS sub_category,
  TRIM(LOWER(market2)) AS market2,
  SAFE_CAST(order_date AS DATE) AS order_date,
  SAFE_CAST(ship_date AS DATE) AS ship_date,
  order_id,
  product_id,
  profit,
  quantity,
  row_id,
  sales,
  shipping_cost,
  discount,
  year,
  weeknum,
FROM `superstorevendas.base_vendas`;

--07. Criação dimensão cliente
CREATE OR REPLACE TABLE `superstorevendas.dim_cliente` AS
SELECT
  customer_id,
  customer_name
FROM (
  SELECT DISTINCT customer_id, customer_name,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_name) AS rn
  FROM `superstorevendas.base_vendas_limpa`
)
WHERE rn = 1;

--08. Criação dimensão ship_mode
CREATE OR REPLACE TABLE `superstorevendas.dim_ship_mode` AS
SELECT
  GENERATE_UUID() AS ship_mode_id,
  ship_mode
FROM (
  SELECT ship_mode,
    ROW_NUMBER() OVER (PARTITION BY ship_mode ORDER BY ship_mode) AS rn
  FROM `superstorevendas.base_vendas_limpa`
)
WHERE rn = 1;

--09. Criação dimensão order_priority
CREATE OR REPLACE TABLE `superstorevendas.dim_order_priority` AS
SELECT
  GENERATE_UUID() AS order_priority_id,
  order_priority
FROM (
  SELECT order_priority,
    ROW_NUMBER() OVER (PARTITION BY order_priority ORDER BY order_priority) AS rn
  FROM `superstorevendas.base_vendas_limpa`
)
WHERE rn = 1;

--10. Criação dimensão categoria
CREATE OR REPLACE TABLE `superstorevendas.dim_categoria` AS
SELECT
  GENERATE_UUID() AS category_id,
  category,
  sub_category
FROM (
  SELECT category, sub_category,
    ROW_NUMBER() OVER (PARTITION BY category, sub_category ORDER BY category) AS rn
  FROM `superstorevendas.base_vendas_limpa`
)
WHERE rn = 1;

--11. Criação dimensão mercado
CREATE OR REPLACE TABLE `superstorevendas.dim_mercado` AS
SELECT
  GENERATE_UUID() AS market_id,
  market,
  market2
FROM (
  SELECT market, market2,
    ROW_NUMBER() OVER (PARTITION BY market, market2 ORDER BY market) AS rn
  FROM `superstorevendas.base_vendas_limpa`
)
WHERE rn = 1;

-- 12. Criação dimensão produto
CREATE OR REPLACE TABLE `superstorevendas.dim_produto` AS
SELECT
  product_id,
  product_name
FROM (
  SELECT DISTINCT product_id, product_name,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_name) AS rn
  FROM `superstorevendas.base_vendas_limpa`
)
WHERE rn = 1;

--13. Criação dimensão regiao
CREATE OR REPLACE TABLE `superstorevendas.dim_regiao` AS
SELECT
  GENERATE_UUID() AS region_id,
  region
FROM (
  SELECT region,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY region) AS rn
  FROM `superstorevendas.base_vendas_limpa`
)
WHERE rn = 1;

--14. Criação dimensão segmento
CREATE OR REPLACE TABLE `superstorevendas.dim_segmento` AS
SELECT
  GENERATE_UUID() AS segment_id,
  segment
FROM (
  SELECT segment,
    ROW_NUMBER() OVER (PARTITION BY segment ORDER BY segment) AS rn
  FROM `superstorevendas.base_vendas_limpa`
)
WHERE rn = 1;

--15. Criação da tabela fato de vendas
CREATE OR REPLACE TABLE `superstorevendas.fato_vendas` AS
SELECT
  GENERATE_UUID() AS fato_id,
  f.order_id,
  f.order_date,
  f.ship_date,
  f.weeknum,
  c.customer_id,
  p.product_id,
  sm.ship_mode_id,
  op.order_priority_id,
  s.segment_id,
  r.region_id,
  m.market_id,
  cat.category_id,
  f.city,
  f.state,
  f.country,
  f.sales,
  f.quantity,
  f.discount,
  f.profit,
  f.shipping_cost,
  EXTRACT(YEAR FROM f.order_date) AS year

FROM `superstorevendas.base_vendas_limpa` f

LEFT JOIN `superstorevendas.dim_cliente` c
  ON f.customer_id = c.customer_id

LEFT JOIN `superstorevendas.dim_produto` p
  ON f.product_id = p.product_id

LEFT JOIN `superstorevendas.dim_ship_mode` sm
  ON f.ship_mode = sm.ship_mode

LEFT JOIN `superstorevendas.dim_order_priority` op
  ON f.order_priority = op.order_priority

LEFT JOIN `superstorevendas.dim_segmento` s
  ON f.segment = s.segment

LEFT JOIN `superstorevendas.dim_regiao` r
  ON f.region = r.region

LEFT JOIN `superstorevendas.dim_mercado` m
  ON f.market = m.market
  AND f.market2 = m.market2

LEFT JOIN `superstorevendas.dim_categoria` cat
  ON f.category = cat.category
  AND f.sub_category = cat.sub_category;

--16. Validação das tabelas
-- Verificar se as quantidades batem
SELECT COUNT(*) AS total_base FROM `superstorevendas.base_vendas_limpa`;
SELECT COUNT(*) AS total_fato FROM `superstorevendas.fato_vendas`;

-- Verificar se há NULLs nas chaves estrangeiras
SELECT COUNT(*) AS nulos_em_dimensoes
FROM `superstorevendas.fato_vendas`
WHERE customer_id IS NULL
  OR product_id IS NULL
  OR ship_mode_id IS NULL
  OR order_priority_id IS NULL
  OR segment_id IS NULL
  OR region_id IS NULL
  OR market_id IS NULL
  OR category_id IS NULL;




