with source as (
    select *
    from public.daily_weather
),

renamed as (
    select
        id,
        city_id,
        date,
        temp_c,
        temp_f,
        condition,
        wind_kph,
        humidity,
        cloud
    from source
)

select * from renamed
