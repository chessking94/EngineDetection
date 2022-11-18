SELECT
ROUND(CAST(m.T1_Eval AS float), 1) AS Eval,
AVG(g.Result) AS Win_Pcnt

FROM lake.Moves m
JOIN lake.Games g ON
	m.GameID = g.GameID
JOIN dim.Sources s ON
	g.SourceID = s.SourceID
JOIN dim.TimeControlDetail td ON
	g.TimeControlDetailID = td.TimeControlDetailID
JOIN dim.TimeControls tc ON
	td.TimeControlID = tc.TimeControlID

WHERE s.SourceName = 'Lichess'
AND tc.TimeControlName = 'Correspondence'
AND (CASE WHEN m.T1_Eval LIKE '#%' THEN 100 ELSE ROUND(CAST(m.T1_Eval AS float), 1) END) BETWEEN -5 AND 5

GROUP BY
ROUND(CAST(m.T1_Eval AS float), 1)

ORDER BY 1
