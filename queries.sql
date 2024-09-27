--Dissolve geometrie: ST_Union_agg()
CREATE TABLE integrated_roads_2024_dissolved AS
	WITH dissolved_roads AS (
		  SELECT 
			Integrated_Road_Class_Descr,
			Integrated_Road_Class_Num,
			CEF_Full_Buffer_Width_Metres,
			CEF_Half_Buffer_Width_Metres,
			ST_Union_agg(geometry) AS geometry
		  FROM 
			integrated_roads_2024_buffer
		  WHERE 
			  MAP_TILE LIKE '114%'
		  GROUP BY 
			Integrated_Road_Class_Descr,
			Integrated_Road_Class_Num,
			CEF_Full_Buffer_Width_Metres,
			CEF_Half_Buffer_Width_Metres
	)
SELECT * FROM dissolved_roads;