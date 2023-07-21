CREATE TABLE components AS 
WITH unique_rows AS (
	SELECT DISTINCT
		word
	,	day
	FROM
		corpus	)

SELECT
	unnest(
		string_to_array(word, '-')
		) AS component
,	word
,	day
FROM
	unique_rows
		
