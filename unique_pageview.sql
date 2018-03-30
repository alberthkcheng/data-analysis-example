-- page_view_with_session Compute Session_ID
WITH page_view_with_session AS(
  SELECT *, 
    SUM(cast(Seconds_Since_Last_Pageview IS NULL OR Seconds_Since_Last_Pageview > 60*10 AS INTEGER))
    OVER (ORDER BY User_ID, Visit_Time)
    AS Session_ID 
  FROM (
    -- TEMP table computes Seconds_Since_Last_Pageview
    SELECT *, 
      unix_timestamp(Visit_Time) - 
      unix_timestamp(
        (lag(Visit_Time) OVER (PARTITION BY User_ID, Visit_Date ORDER BY  Visit_Time))
      ) Seconds_Since_Last_Pageview
    FROM sample_pageview
  )
)
-- DISTINCT IS USED So that it is countint unique pageview instead of total pageview
SELECT Page_ID, Visit_Date, COUNT(DISTINCT Session_ID) AS Total_User_Sessions
FROM page_view_with_session
GROUP BY Page_ID, Visit_Date
ORDER BY Page_ID, Visit_Date
