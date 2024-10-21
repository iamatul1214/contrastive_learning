''' This code shows how you can calculate the velcotiy features on a training dataset for model training'''

def perform_feature_engineering_flights_olap_data(df: DataFrame, start_date: str, departure_end_date: str) -> DataFrame:
    sc_wind = Window.partitionBy('source_code').orderBy(F.unix_timestamp('created_at'))
    dc_wind = Window.partitionBy('destination_code').orderBy(F.unix_timestamp('created_at'))
    cid_wind = Window.partitionBy('customer_id').orderBy(F.unix_timestamp('created_at'))
    ac_wind = Window.partitionBy('airline_code').orderBy(F.unix_timestamp('created_at'))
    rt_wind = Window.partitionBy('route').orderBy(F.unix_timestamp('created_at'))

    # 1M Window
    sc_wind_1m = sc_wind.rangeBetween(-30 * 24 * 3600, -1)
    dc_wind_1m = dc_wind.rangeBetween(-30 * 24 * 3600, -1)
    cid_wind_1m = cid_wind.rangeBetween(-30 * 24 * 3600, -1)
    ac_wind_1m = ac_wind.rangeBetween(-30 * 24 * 3600, -1)
    rt_wind_1m = rt_wind.rangeBetween(-30 * 24 * 3600, -1)

    # 6M Window
    sc_wind_6m = sc_wind.rangeBetween(-6 * 30 * 24 * 3600, -1)
    dc_wind_6m = dc_wind.rangeBetween(-6 * 30 * 24 * 3600, -1)
    cid_wind_6m = cid_wind.rangeBetween(-6 * 30 * 24 * 3600, -1)
    ac_wind_6m = ac_wind.rangeBetween(-6 * 30 * 24 * 3600, -1)
    rt_wind_6m = rt_wind.rangeBetween(-30 * 24 * 3600, -1)

    cp_cancel_colm = F.when(F.col('isCPTicket'), F.col('is_ticket_cancelled'))

    df_feats = (
        df
        .select(
            'order_id',
            'customer_id',
            'channel_id',
            'total_passenger_count',
            'child_count',
            'infant_count',
            'corporate_and_normal_booking_segregation',
            'number_of_segments',
            'number_of_hops',
            'airline_name',
            'airline_code',
            'created_at',
            'is_ticket_cancelled',
            'selling_price',
            'cp_premium_price',
            'conv_fee',
            'source_code',
            'destination_code',
            'pd_created_at',
            'pd_claim_date',
            'pd_policy_status',
            'isCPTicket',
            F.month(F.col('created_at')).alias("booking_month"),
            F.dayofweek(F.col('created_at')).alias("booking_day_of_week"),
            F.dayofmonth(F.col('created_at')).alias("booking_date"),
            F.month(F.col('arrival_time')).alias("arrival_month"),
            F.dayofweek(F.col('arrival_time')).alias("arrival_day_of_week"),
            F.dayofmonth(F.col('arrival_time')).alias("arrival_date"),
            F.hour(F.col('arrival_time')).alias("arrival_hour"),
            F.month(F.col('departure_time')).alias("departure_month"),
            F.dayofweek(F.col('departure_time')).alias("departure_day_of_week"),
            F.dayofmonth(F.col('departure_time')).alias("departure_date"),
            F.hour(F.col('departure_time')).alias("departure_hour"),
            F.concat(F.col('source_code'), F.lit("-"), F.col('destination_code')).alias("route"),
            F.when(F.col("is_refundable") == "TRUE", 1).otherwise(0).alias('is_refundable'),
            F.round((F.unix_timestamp(F.col('departure_time')).cast('long') - F.unix_timestamp(
                F.col('created_at')).cast('long')) / F.lit(3600), 2).alias("booking_dep_diff_hours"),
            convert_duration_to_minutes(F.col('duration')).alias("duration_in_mins"),

            # Create Cancellation Rate Vars
            F.mean('is_ticket_cancelled').over(sc_wind_1m).alias('source_code_cancel_rate_1M'),
            F.mean('is_ticket_cancelled').over(sc_wind_6m).alias('source_code_cancel_rate_6M'),

            F.mean('is_ticket_cancelled').over(dc_wind_1m).alias('destination_code_cancel_rate_1M'),
            F.mean('is_ticket_cancelled').over(dc_wind_6m).alias('destination_code_cancel_rate_6M'),

            F.mean('is_ticket_cancelled').over(cid_wind_1m).alias('customer_id_cancel_rate_1M'),
            F.mean('is_ticket_cancelled').over(cid_wind_6m).alias('customer_id_cancel_rate_6M'),

            F.mean('is_ticket_cancelled').over(ac_wind_1m).alias('airline_code_cancel_rate_1M'),
            F.mean('is_ticket_cancelled').over(ac_wind_6m).alias('airline_code_cancel_rate_6M'),

            # Create Cancellation Rate Vars for CP
            F.mean(cp_cancel_colm).over(sc_wind_1m).alias('source_code_cp_cancel_rate_1M'),
            F.mean(cp_cancel_colm).over(sc_wind_6m).alias('source_code_cp_cancel_rate_6M'),

            F.mean(cp_cancel_colm).over(dc_wind_1m).alias('destination_code_cp_cancel_rate_1M'),
            F.mean(cp_cancel_colm).over(dc_wind_6m).alias('destination_code_cp_cancel_rate_6M'),

            F.mean(cp_cancel_colm).over(cid_wind_1m).alias('customer_id_cp_cancel_rate_1M'),
            F.mean(cp_cancel_colm).over(cid_wind_6m).alias('customer_id_cp_cancel_rate_6M'),

            F.mean(cp_cancel_colm).over(ac_wind_1m).alias('airline_code_cp_cancel_rate_1M'),
            F.mean(cp_cancel_colm).over(ac_wind_6m).alias('airline_code_cp_cancel_rate_6M'),

            # Create Counter Variables.
            F.count('order_id').over(cid_wind_1m).alias('cust_tickets_booked_1M'),
            F.count('order_id').over(cid_wind_6m).alias('cust_tickets_booked_6M'),
            F.count('pd_cart_item_id').over(cid_wind_1m).alias('cust_cp_tickets_booked_1M'),
            F.count('pd_cart_item_id').over(cid_wind_6m).alias('cust_cp_tickets_booked_6M'),

            F.sum('cp_premium_price').over(cid_wind_6m).alias('cust_cp_GMV_6M'),
            F.sum('selling_price').over(cid_wind_6m).alias('cust_flights_GMV_6M'),
        )
        .select(
            '*',
            F.mean('is_ticket_cancelled').over(rt_wind_1m).alias('route_cancel_rate_1M'),
            F.mean('is_ticket_cancelled').over(rt_wind_6m).alias('route_cancel_rate_6M'),
            F.mean(cp_cancel_colm).over(rt_wind_1m).alias('route_cp_cancel_rate_1M'),
            F.mean(cp_cancel_colm).over(rt_wind_6m).alias('route_cp_cancel_rate_6M'),
            F.size(F.collect_set('route').over(cid_wind_6m)).alias('cust_routes_taken_6M'),
        )
        .where(F.to_date("pd_created_at").isNotNull())
        .where(F.to_date("pd_created_at").between(start_date, departure_end_date))
        .where(F.to_date("departure_time").between(start_date, departure_end_date))
    )

    return df_feats