-- transformations for green taxis
with base as (
    select
        'green' as cab_type,
        lpep_pickup_datetime as pickup_datetime,
        lpep_dropoff_datetime as dropoff_datetime,
        passenger_count,
        trip_distance,
        (trip_distance * e.co2_grams_per_mile / 1000.0) as trip_co2_kgs,
        case 
            when (epoch(lpep_dropoff_datetime) - epoch(lpep_pickup_datetime)) > 0
            then (trip_distance / ((epoch(lpep_dropoff_datetime) - epoch(lpep_pickup_datetime)) / 3600.0))
            else null
        end as avg_mph,
        extract(hour from lpep_pickup_datetime) as hour_of_day,
        extract(dow from lpep_pickup_datetime) as day_of_week,
        extract(week from lpep_pickup_datetime) as week_of_year,
        extract(month from lpep_pickup_datetime) as month_of_year
    from green t
    join {{ source('main','vehicle_emissions') }} e
      on e.vehicle_type = 'green_taxi'
)
select * from base
