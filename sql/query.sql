CREATE TABLE IF NOT EXISTS `andika-coba.mekari.salary_per_hour_sql` AS

WITH distincted_employees AS (
  SELECT
    employee_id,
    branch_id,
    MAX(salary) AS salary
  FROM
    `andika-coba.mekari.employees`
  GROUP BY
    employee_id,
    branch_id
),

joined_data AS (SELECT DISTINCT
  EXTRACT(YEAR from ts.date) AS year,
  EXTRACT(MONTH from ts.date) AS month,
  EXTRACT(DAY from ts.date) AS day,
  em.branch_id,
  em.employee_id,
  em.salary,
  TIMESTAMP_DIFF(
    TIMESTAMP(CONCAT(CAST(ts.date AS STRING), ' ', CAST(ts.checkout AS STRING))),
    TIMESTAMP(CONCAT(CAST(ts.date AS STRING), ' ', CAST(ts.checkin AS STRING))),
    HOUR
  ) AS hour_spent,
FROM
  `andika-coba.mekari.timesheets` ts
LEFT JOIN
  distincted_employees em
ON
  ts.employee_id = em.employee_id
WHERE
  ts.checkin IS NOT NULL AND ts.checkout IS NOT NULL),


cost_per_employee AS (SELECT
  year,
  month,
  branch_id,
  employee_id,
  MAX(salary) AS salary,
  SUM(hour_spent) AS total_hour_spent
FROM
  joined_data
WHERE
  hour_spent > 0
GROUP BY
  year,
  month,
  branch_id,
  employee_id)

SELECT
  year,
  month,
  branch_id,
  CAST(SUM(salary)/SUM(total_hour_spent) AS INT64) AS salary_per_hour
FROM
  cost_per_employee
GROUP BY
  year,
  month,
  branch_id
ORDER BY
  year,
  month,
  branch_id
