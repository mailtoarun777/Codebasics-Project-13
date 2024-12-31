##Task 1:

SELECT
    dc.city_name,                                                                                 -- Gives the city names
    COUNT(ft.trip_id) AS total_trips,                                                             -- while it counts total number of trips in the table 
    ROUND(AVG(ft.fare_amount / NULLIF(ft.distance_travelled_km, 0)), 2) AS avg_fare_per_km,       -- By doing AVG of fare_amount / distance_travelled_km we get avg fare per KM , NULLIF is used for safe division
    ROUND(AVG(ft.fare_amount), 2) AS avg_fare_per_trip,                                           -- to Find avg amount we do AVG of fare amount
    CONCAT(                                            											  -- Concat funtion for adding " % " at the end to better understanding
        ROUND(
            (COUNT(ft.trip_id) * 100.0 / SUM(COUNT(ft.trip_id)) OVER()), 						  -- counting each tripid and dividing by total tripid will give percentage contribution for each cities as we have grouping function at the end of the query
            2
        ), '%'
    ) AS contribution_to_total_trips_pct                                                          
FROM
    fact_trips ft														
JOIN 
    dim_city dc ON ft.city_id = dc.city_id														 -- Joined on city_id	
GROUP BY
    dc.city_name																				 -- I used GROUP function to get the result in city wise
ORDER BY
    total_trips DESC;                                                                            -- Order to see the result to see from highest to lowest based on total trips

##Task 2:
    
    SELECT 
    dc.city_name,                                                                        -- to retrive city name
    dd.month_name,                                                                       -- to get month name
    COUNT(ft.trip_id) AS actual_trips,                                                   -- to count actual total tips
    tmt.total_target_trips AS target_trips,                                              -- to get targets for each city and months
    CASE
        WHEN COUNT(ft.trip_id) > tmt.total_target_trips THEN 'Met Target'              -- condition for Met target
        ELSE 'Not Met Target'                                                              -- condition for Not Met target
    END AS performance_status,                                                           
    CONCAT( 																			 -- using cancat function to show "%" at the end
    ROUND(
        ((COUNT(ft.trip_id) - tmt.total_target_trips) * 100.0 / tmt.total_target_trips), 
        2),
        "%"
    ) AS pct_difference                                                                 -- percentage difference between actual and target trips
FROM
    fact_trips ft                                                                        -- this is fact table containing trip data
JOIN
    dim_city dc ON ft.city_id = dc.city_id                                               -- joining to get city names
JOIN
    dim_date dd ON ft.date = dd.date                                                     -- joining to get month names
JOIN
    targets_db.monthly_target_trips tmt 												 -- i used (targets_db.monthly_target_trips) to define a different database
    ON dc.city_id = tmt.city_id                                                          -- joining to get target trips data for each city
    AND dd.start_of_month = tmt.month                                                    -- also joining month to get month name
GROUP BY 
    dc.city_name,                                                                        -- group by city name
    dd.month_name,                                                                       -- group by month name
    tmt.total_target_trips,                                                              -- group by target trips for the city and month
    dd.start_of_month                                                                    -- group by start of the month for sorting
ORDER BY 
    dc.city_name,                                                                        -- order by city name first
    MONTH(dd.start_of_month);                                                            -- then order by the calender value of the month (jan = 1, feb = 2,....)


##Task3:

WITH total_repeat_passengers AS (                                                             -- cte for better readability
    SELECT
        f.city_id,
        f.month,
        SUM(f.repeat_passengers) AS total_repeat_passengers
    FROM fact_passenger_summary f
    GROUP BY f.city_id, f.month                                                               -- this gives total of repeat passengers
),
repeat_trip_counts AS (
    SELECT                                                                                    -- calculates Trip wise. eg;( 2 trips = 1000 , 3 trips = 2000) 
        d.city_id,
        d.month,
        SUM(CASE WHEN d.trip_count = '2-Trips' THEN d.repeat_passenger_count ELSE 0 END) AS `2-Trips`,
        SUM(CASE WHEN d.trip_count = '3-Trips' THEN d.repeat_passenger_count ELSE 0 END) AS `3-Trips`,
        SUM(CASE WHEN d.trip_count = '4-Trips' THEN d.repeat_passenger_count ELSE 0 END) AS `4-Trips`,
        SUM(CASE WHEN d.trip_count = '5-Trips' THEN d.repeat_passenger_count ELSE 0 END) AS `5-Trips`,
        SUM(CASE WHEN d.trip_count = '6-Trips' THEN d.repeat_passenger_count ELSE 0 END) AS `6-Trips`,
        SUM(CASE WHEN d.trip_count = '7-Trips' THEN d.repeat_passenger_count ELSE 0 END) AS `7-Trips`,
        SUM(CASE WHEN d.trip_count = '8-Trips' THEN d.repeat_passenger_count ELSE 0 END) AS `8-Trips`,
        SUM(CASE WHEN d.trip_count = '9-Trips' THEN d.repeat_passenger_count ELSE 0 END) AS `9-Trips`,
        SUM(CASE WHEN d.trip_count = '10-Trips' THEN d.repeat_passenger_count ELSE 0 END) AS `10-Trips`
    FROM dim_repeat_trip_distribution d
    GROUP BY d.city_id, d.month
)
SELECT
    c.city_name,                                                                               -- by dividing each trips by total we get the % contribution
    ROUND((SUM(r.`2-Trips`) / SUM(t.total_repeat_passengers)) * 100, 2) AS `2-Trips (%)`,
    ROUND((SUM(r.`3-Trips`) / SUM(t.total_repeat_passengers)) * 100, 2) AS `3-Trips (%)`,
    ROUND((SUM(r.`4-Trips`) / SUM(t.total_repeat_passengers)) * 100, 2) AS `4-Trips (%)`,
    ROUND((SUM(r.`5-Trips`) / SUM(t.total_repeat_passengers)) * 100, 2) AS `5-Trips (%)`,
    ROUND((SUM(r.`6-Trips`) / SUM(t.total_repeat_passengers)) * 100, 2) AS `6-Trips (%)`,
    ROUND((SUM(r.`7-Trips`) / SUM(t.total_repeat_passengers)) * 100, 2) AS `7-Trips (%)`,
    ROUND((SUM(r.`8-Trips`) / SUM(t.total_repeat_passengers)) * 100, 2) AS `8-Trips (%)`,
    ROUND((SUM(r.`9-Trips`) / SUM(t.total_repeat_passengers)) * 100, 2) AS `9-Trips (%)`,
    ROUND((SUM(r.`10-Trips`) / SUM(t.total_repeat_passengers)) * 100, 2) AS `10-Trips (%)`
FROM total_repeat_passengers t
JOIN repeat_trip_counts r ON t.city_id = r.city_id AND t.month = r.month                      -- joining 1st and 2nd cte
JOIN dim_city c ON r.city_id = c.city_id													  -- joining city table to get city names
GROUP BY c.city_name
ORDER BY c.city_name;


##Task4:

WITH city_passenger_totals AS (                                            -- cte for better readability            
    SELECT 
        dc.city_name,                                                      -- gives city name
        SUM(fp.new_passengers) AS total_new_passengers                     -- sum total new passengers for each city
    FROM 
        fact_passenger_summary fp                                    
    JOIN 
        dim_city dc ON fp.city_id = dc.city_id             
    GROUP BY 
        dc.city_name                                       
),                                                                         -- this table shows city and total new passengers

ranked_cities AS (
    SELECT 
        city_name,
        total_new_passengers,
        RANK() OVER (ORDER BY total_new_passengers DESC) AS rank_highest,  -- cities ranked in descending order : highest passengers
        RANK() OVER (ORDER BY total_new_passengers ASC) AS rank_lowest     -- cities ranked in ascending order : lowest passengers
    FROM 
        city_passenger_totals
),                                                                         -- this table add ranks to the new passengers 

categorized_cities AS (
    SELECT 
        city_name,
        total_new_passengers,
        CASE 
            WHEN rank_highest <= 3 THEN 'Top 3'                           -- gives top 3 cities based on the highest number of passengers
            WHEN rank_lowest <= 3 THEN 'Bottom 3'                         -- give bottom 3 cities based on the lowest number of passengers
            ELSE NULL                                                     -- other citites become null
        END AS city_category                                              
    FROM 
        ranked_cities
)


SELECT 
    city_name,
    total_new_passengers,
    city_category                                         
FROM 
    categorized_cities
WHERE 
    city_category IS NOT NULL                                             -- gives only the Top 3 and Bottom 3 cities
ORDER BY 
	total_new_passengers DESC;                            
    
##Task5:

WITH monthly_city_revenue AS (											 -- cte for better readability
    SELECT 
        dc.city_name,                                                    
        dd.month_name,                                                   
        SUM(ft.fare_amount) AS monthly_revenue                           
    FROM 
        fact_trips ft                                                   
    JOIN 
        dim_city dc ON ft.city_id = dc.city_id         
    JOIN 
        dim_date dd ON ft.date = dd.date               
    GROUP BY 
        dc.city_name, dd.month_name                   
),                                                                      -- this table gives city , months and revenue


city_total_revenue AS (												     
    SELECT 
        city_name,
        SUM(monthly_revenue) AS total_city_revenue    
    FROM 
        monthly_city_revenue
    GROUP BY 
        city_name
),                                                                       -- this table give city and its monthly revenue


city_highest_revenue_month AS (                                          
    SELECT 
        mcr.city_name,
        mcr.month_name AS highest_revenue_month,      
        mcr.monthly_revenue AS revenue,               
        ROUND((mcr.monthly_revenue * 100.0 / ctr.total_city_revenue), 2) AS percentage_contribution  -- give % contribution
                                                     
    FROM 
        monthly_city_revenue mcr
    JOIN 
        city_total_revenue ctr ON mcr.city_name = ctr.city_name            -- joining to calculate percentage contribution
    WHERE 
        mcr.monthly_revenue = (                                           
            SELECT MAX(monthly_revenue)
            FROM monthly_city_revenue sub_mcr
            WHERE sub_mcr.city_name = mcr.city_name                        -- filters to get the highest revenue month for each city
        )
)

SELECT 
    city_name,                                                           
    highest_revenue_month,                                               
    revenue,                                                             
    percentage_contribution     
FROM 
    city_highest_revenue_month
ORDER BY 
    revenue DESC;                                                           

##Task6:

WITH aggregated_table AS (
    SELECT 
        dc.city_name, 
        dd.month_name,
        fps.city_id, 
        SUM(fps.total_passengers) AS total_passengers, 
        SUM(fps.repeat_passengers) AS repeat_passengers
    FROM 
        fact_passenger_summary fps
    JOIN 
        dim_city dc ON fps.city_id = dc.city_id
    JOIN 
        dim_date dd ON fps.month = dd.start_of_month
    GROUP BY 
        dc.city_name, dd.month_name, fps.city_id
)                                                                                        -- this ia a aggregated table give over all data for further calculations
SELECT 
    ad.city_name,
    ad.month_name,
    ad.total_passengers,
    ad.repeat_passengers,
    ROUND(
        COALESCE(ad.repeat_passengers * 100.0 / ad.total_passengers, 0), 2               -- to get monthly repeat passengers rate
    ) AS monthly_repeat_passenger_rate,                                
    ROUND(
        COALESCE(
            SUM(ad.repeat_passengers) OVER (PARTITION BY ad.city_name) * 100.0 / 
            SUM(ad.total_passengers) OVER (PARTITION BY ad.city_name),                   -- to get city wise repeat passenger rate
            0
        ), 2
    ) AS city_repeat_passenger_rate
FROM 
    aggregated_table ad
ORDER BY 
    ad.city_name, ad.month_name;
