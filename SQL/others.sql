SELECT COUNT(DISTINCT (groupid, artifactname))
FROM data
WHERE versionscheme='6' AND classifier IS NULL AND timestamp > '2004-01-01 00:00:00'

SELECT COUNT(DISTINCT (groupid, artifactname)), EXTRACT(YEAR from timestamp) AS year
FROM data
WHERE versionscheme='6' AND classifier IS NULL AND timestamp > '2004-01-01 00:00:00'
GROUP BY year
ORDER BY year
