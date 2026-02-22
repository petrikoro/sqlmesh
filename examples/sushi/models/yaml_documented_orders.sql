MODEL (
  name sushi.yaml_documented_orders,
  kind FULL,
  owner jen,
  cron '@daily'
);

SELECT
  id::INT AS order_id,
  customer_id::INT AS customer_id,
  event_date::DATE AS event_date
FROM sushi.orders
