SELECT count(*)
FROM data
WHERE classifier is null
GROUP BY versionscheme

SELECT count(*)
FROM data
WHERE classifier is null AND timestamp > '2004-01-01 00:00:00'
GROUP BY versionscheme

select EXTRACT(YEAR FROM timestamp) AS year, versionscheme, count(*)
from data
where classifier is null AND timestamp > '2004-01-01 00:00:00'
group by versionscheme, year
ORDER BY year, versionscheme
