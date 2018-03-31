-- page_view_with_session compute Session_ID
WITH page_view_with_session AS(
  SELECT *, 
    -- It is a new session if it is user's 1st activity or last activity is more than 10 mins ago.
    SUM(CAST(Seconds_Since_Last_Pageview IS NULL OR Seconds_Since_Last_Pageview > 60*10 AS INTEGER))
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
-- SELECT * FROM page_view_with_session LIMIT 100 -- Uncomment to test page_view_with_session table
-- DISTINCT is used so that it is counting unique pageview instead of total pageview
SELECT Page_ID, Visit_Date, COUNT(DISTINCT Session_ID) AS Total_User_Sessions
FROM page_view_with_session
GROUP BY Page_ID, Visit_Date
ORDER BY Page_ID, Visit_Date
