with base as (
    select
        date,
        city_id,
        avg(temp_c) as avg_temp_c,
        avg(humidity) as avg_humidity,
        avg(wind_kph) as avg_wind,
        avg(cloud) as avg_cloud
    from {{ ref('stg_weather') }}
    group by 1, 2
)

select * from base
