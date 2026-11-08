SELECT sum(id) AS actual
FROM {{ ref('generated_numbers') }}
HAVING sum(id) != 45
