 '''
 This file describes how to make feature on run time for that day which means on scoring day, not exactly like training code. That's why we have used days between 
 because I am not creating any window on a larger training dataset, where I can calculate the number of push sms sent in last 7 days and the data would have used window.partition on date 
 column, and we would have got various values of last 7 days. But here it is on scoring day so that last 7 days need not to be in windows.partitionby because we have only 1 value of last 7 day
 because we know the value of scoring day already. And there can be only one scoring day.
 '''


  ## Define flags
is_push = F.col('event_name') == 'PUSH'
is_push_delivered = F.col('sms_delivery_status').isin(['pushNotificationDisplayed',
                                                                'pushNotificationClicked',
                                                                'pushNotificationDismissed',
                                                                'pushNotificationSoundPlayed',
                                                                'pushNotificationReceived',
                                                                'duplicatePushNotificationReceived',
                                                                'silentPushReceived'])
is_push_responded = F.col('sms_delivery_status').isin(['pushNotificationDisplayed', 'pushNotificationClicked'])


feats_def = self.get_feature_list(end_date_dt)
perc_feats_def = self.get_perc_features()
 
 (
            df_event_log_ct
            .groupby(['loan_book_id'])
            .agg(*feats_def)
            .select('*', *perc_feats_def)
            # .select('loan_book_id', *feature_list)
            .write.mode('overwrite').parquet(ct_feat_path)
        )
 
 
 def get_feature_list(self, end_date_dt: date) -> list[Column]:
    
        """This function returns the feature column functions"""
        # NULL
        NULL = F.lit(None)

        # Calculate lookback datetime objects
        days_d = Utils.get_days_dict(end_date_dt, [1, 3, 7, 31, 62, 90])

        # TIME FILTERS
        time_01d = F.col('dt').between(days_d[1], end_date_dt)
        time_03d = F.col('dt').between(days_d[3], end_date_dt)
        time_07d = F.col('dt').between(days_d[7], end_date_dt)
        # time_01m = F.col('dt').between(days_d[31], end_date_dt)
        # time_02m = F.col('dt').between(days_d[62], end_date_dt)
        # time_03m = F.col('dt').between(days_d[90], end_date_dt)

        feature_list = [

            cnt_distinct(F.when(time_01d, F.col("event_id")).otherwise(NULL)).alias("push_count_01d"),
            cnt_distinct(F.when(time_03d, F.col("event_id")).otherwise(NULL)).alias("push_count_03d"),
            cnt_distinct(F.when(time_07d, F.col("event_id")).otherwise(NULL)).alias("push_count_07d"),
            # cnt_distinct(F.when(time_01m, F.col("event_id")).otherwise(NULL)).alias("push_count_01m"),
            # cnt_distinct(F.when(time_02m, F.col("event_id")).otherwise(NULL)).alias("push_count_02m"),
            # cnt_distinct(F.when(time_03m, F.col("event_id")).otherwise(NULL)).alias("push_count_03m"),

            cnt_distinct(F.when(time_01d, F.col("is_push_delivered")).otherwise(NULL)).alias("push_delivered_count_01d"),
            cnt_distinct(F.when(time_03d, F.col("is_push_delivered")).otherwise(NULL)).alias("push_delivered_count_03d"),
            cnt_distinct(F.when(time_07d, F.col("is_push_delivered")).otherwise(NULL)).alias("push_delivered_count_07d"),
            # cnt_distinct(F.when(time_01m, F.col("is_push_delivered")).otherwise(NULL)).alias("push_delivered_count_01m"),
            # cnt_distinct(F.when(time_02m, F.col("is_push_delivered")).otherwise(NULL)).alias("push_delivered_count_02m"),
            # cnt_distinct(F.when(time_03m, F.col("is_push_delivered")).otherwise(NULL)).alias("push_delivered_count_03m"),

            cnt_distinct(F.when(time_01d, F.col("is_push_responded")).otherwise(NULL)).alias("push_responded_count_01d"),
            cnt_distinct(F.when(time_03d, F.col("is_push_responded")).otherwise(NULL)).alias("push_responded_count_03d"),
            cnt_distinct(F.when(time_07d, F.col("is_push_responded")).otherwise(NULL)).alias("push_responded_count_07d"),
            # cnt_distinct(F.when(time_01m, F.col("is_push_responded")).otherwise(NULL)).alias("push_responded_count_01m"),
            # cnt_distinct(F.when(time_02m, F.col("is_push_responded")).otherwise(NULL)).alias("push_responded_count_02m"),
            # cnt_distinct(F.when(time_03m, F.col("is_push_responded")).otherwise(NULL)).alias("push_responded_count_03m"),
            
        ]

        return feature_list
        
    def get_perc_features(self) -> list[Column]:
        """This function does the transformation for the ratio/percent columns"""
        perc_features = [

        F.when(F.col('push_count_01d') > 0, F.col('push_responded_count_01d') / F.col('push_count_01d')).otherwise(-1).alias('push_response_rate_01d'),
        F.when(F.col('push_count_03d') > 0, F.col('push_responded_count_03d') / F.col('push_count_03d')).otherwise(-1).alias('push_response_rate_03d'),
        F.when(F.col('push_count_07d') > 0, F.col('push_responded_count_07d') / F.col('push_count_07d')).otherwise(-1).alias('push_response_rate_07d'),
        # F.when(F.col('push_count_01m') > 0, F.col('push_responded_count_01m') / F.col('push_count_01m')).otherwise(-1).alias('push_response_rate_01m'),
        # F.when(F.col('push_count_02m') > 0, F.col('push_responded_count_02m') / F.col('push_count_02m')).otherwise(-1).alias('push_response_rate_02m'),
        # F.when(F.col('push_count_03m') > 0, F.col('push_responded_count_03m') / F.col('push_count_03m')).otherwise(-1).alias('push_response_rate_03m'),

        F.when(F.col('push_count_01d') > 0, F.col('push_delivered_count_01d') / F.col('push_count_01d')).otherwise(-1).alias('push_delivery_rate_01d'),
        F.when(F.col('push_count_03d') > 0, F.col('push_delivered_count_03d') / F.col('push_count_03d')).otherwise(-1).alias('push_delivery_rate_03d'),
        F.when(F.col('push_count_07d') > 0, F.col('push_delivered_count_07d') / F.col('push_count_07d')).otherwise(-1).alias('push_delivery_rate_07d'),
        # F.when(F.col('push_count_01m') > 0, F.col('push_delivered_count_01m') / F.col('push_count_01m')).otherwise(-1).alias('push_delivery_rate_01m'),
        # F.when(F.col('push_count_02m') > 0, F.col('push_delivered_count_02m') / F.col('push_count_02m')).otherwise(-1).alias('push_delivery_rate_02m'),
        # F.when(F.col('push_count_03m') > 0, F.col('push_delivered_count_03m') / F.col('push_count_03m')).otherwise(-1).alias('push_delivery_rate_03m'),

        ]
        return perc_features