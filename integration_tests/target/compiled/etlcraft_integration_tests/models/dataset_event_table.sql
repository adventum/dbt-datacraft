


  
  
  
  
    SELECT * FROM (

        (
            select

                toLowCardinality('full_datestat')  as _dbt_source_relation,
                
                            toString("None") as None ,
                            toDate("__date") as __date ,
                            toString("reportType") as reportType ,
                            toString("accountName") as accountName ,
                            toString("__table_name") as __table_name ,
                            toString("adSourceDirty") as adSourceDirty ,
                            toString("productName") as productName ,
                            toString("adCampaignName") as adCampaignName ,
                            toString("adGroupName") as adGroupName ,
                            toString("adId") as adId ,
                            toString("adPhraseId") as adPhraseId ,
                            toString("utmSource") as utmSource ,
                            toString("utmMedium") as utmMedium ,
                            toString("utmCampaign") as utmCampaign ,
                            toString("utmTerm") as utmTerm ,
                            toString("utmContent") as utmContent ,
                            toString("utmHash") as utmHash ,
                            toString("adTitle1") as adTitle1 ,
                            toString("adTitle2") as adTitle2 ,
                            toString("adText") as adText ,
                            toString("adPhraseName") as adPhraseName ,
                            toFloat64("adCost") as adCost ,
                            toInt32("impressions") as impressions ,
                            toInt32("clicks") as clicks ,
                            toDateTime("__emitted_at") as __emitted_at ,
                            toString("__link") as __link ,
                            toString("AdCostStatHash") as AdCostStatHash ,
                            toString("__id") as __id ,
                            toDateTime("__datetime") as __datetime ,
                            toInt64(0) as __period_number ,
                            toBool(0) as __if_missed ,
                            toUInt16(0) as __priority ,
                            toNullable('') as __step ,
                            toUInt64(0) as qid ,
                            toDateTime(0) as event_datetime ,
                            toString('') as appmetricaDeviceId ,
                            toString('') as mobileAdsId ,
                            toString('') as crmUserId ,
                            toString('') as visitId ,
                            toString('') as clientId ,
                            toString('') as promoCode ,
                            toString('') as osName ,
                            toString('') as cityName ,
                            toString('') as transactionId ,
                            toUInt8(0) as sessions ,
                            toUInt8(0) as addToCartSessions ,
                            toUInt8(0) as cartViewSessions ,
                            toUInt8(0) as checkoutSessions ,
                            toUInt8(0) as webSalesSessions ,
                            toUInt8(0) as sales ,
                            toNullable(0) as amountSales ,
                            toUInt8(0) as registrationCardSessions ,
                            toUInt8(0) as registrationButtonClick ,
                            toUInt8(0) as linkingCardToPhoneNumberSessions ,
                            toUInt8(0) as registrationLendingPromotionsSessions ,
                            toUInt8(0) as registrationCashbackSessions ,
                            toUInt8(0) as instantDiscountActivationSessions ,
                            toUInt8(0) as couponActivationSessions ,
                            toUInt8(0) as participationInLotterySessions ,
                            toUInt8(0) as pagesViews ,
                            toUInt64(0) as screenView ,
                            toUInt8(0) as installApp ,
                            toUInt8(0) as installs ,
                            toString('') as installationDeviceId ,
                            toString('') as cityCode ,
                            toUInt32(0) as pageViews ,
                            toString('') as AppInstallStatHash ,
                            toString('') as AppEventStatHash ,
                            toString('') as AppSessionStatHash ,
                            toString('') as AppDeeplinkStatHash ,
                            toString('') as VisitStatHash ,
                            toString('') as AppMetricaDeviceHash ,
                            toString('') as CrmUserHash ,
                            toString('') as YmClientHash ,
                            toUInt8(0) as __last_click_rank ,
                            toUInt8(0) as __first_click_rank ,
                            toString('') as __myfirstfunnel_last_click_utmSource ,
                            toString('') as __myfirstfunnel_last_click_utmMedium ,
                            toString('') as __myfirstfunnel_last_click_utmCampaign ,
                            toString('') as __myfirstfunnel_last_click_utmTerm ,
                            toString('') as __myfirstfunnel_last_click_utmContent ,
                            toString('') as __myfirstfunnel_last_click_adSourceDirty ,
                            toString('') as __myfirstfunnel_first_click_utmSource ,
                            toString('') as __myfirstfunnel_first_click_utmMedium ,
                            toString('') as __myfirstfunnel_first_click_utmCampaign ,
                            toString('') as __myfirstfunnel_first_click_utmTerm ,
                            toString('') as __myfirstfunnel_first_click_utmContent ,
                            toString('') as __myfirstfunnel_first_click_adSourceDirty 

            from test.full_datestat
        )

        union all
        

        (
            select

                toLowCardinality('attr_myfirstfunnel_final_table')  as _dbt_source_relation,
                
                            toString("None") as None ,
                            toDate("__date") as __date ,
                            toString('') as reportType ,
                            toString("accountName") as accountName ,
                            toString("__table_name") as __table_name ,
                            toString("adSourceDirty") as adSourceDirty ,
                            toString('') as productName ,
                            toString('') as adCampaignName ,
                            toString('') as adGroupName ,
                            toString('') as adId ,
                            toString('') as adPhraseId ,
                            toString("utmSource") as utmSource ,
                            toString("utmMedium") as utmMedium ,
                            toString("utmCampaign") as utmCampaign ,
                            toString("utmTerm") as utmTerm ,
                            toString("utmContent") as utmContent ,
                            toString("utmHash") as utmHash ,
                            toString('') as adTitle1 ,
                            toString('') as adTitle2 ,
                            toString('') as adText ,
                            toString('') as adPhraseName ,
                            toFloat64(0) as adCost ,
                            toInt32(0) as impressions ,
                            toInt32(0) as clicks ,
                            toDateTime("__emitted_at") as __emitted_at ,
                            toString("__link") as __link ,
                            toString('') as AdCostStatHash ,
                            toString("__id") as __id ,
                            toDateTime("__datetime") as __datetime ,
                            toInt64("__period_number") as __period_number ,
                            toBool("__if_missed") as __if_missed ,
                            toUInt16("__priority") as __priority ,
                            toNullable("__step") as __step ,
                            toUInt64("qid") as qid ,
                            toDateTime("event_datetime") as event_datetime ,
                            toString("appmetricaDeviceId") as appmetricaDeviceId ,
                            toString("mobileAdsId") as mobileAdsId ,
                            toString("crmUserId") as crmUserId ,
                            toString("visitId") as visitId ,
                            toString("clientId") as clientId ,
                            toString("promoCode") as promoCode ,
                            toString("osName") as osName ,
                            toString("cityName") as cityName ,
                            toString("transactionId") as transactionId ,
                            toUInt8("sessions") as sessions ,
                            toUInt8("addToCartSessions") as addToCartSessions ,
                            toUInt8("cartViewSessions") as cartViewSessions ,
                            toUInt8("checkoutSessions") as checkoutSessions ,
                            toUInt8("webSalesSessions") as webSalesSessions ,
                            toUInt8("sales") as sales ,
                            toNullable("amountSales") as amountSales ,
                            toUInt8("registrationCardSessions") as registrationCardSessions ,
                            toUInt8("registrationButtonClick") as registrationButtonClick ,
                            toUInt8("linkingCardToPhoneNumberSessions") as linkingCardToPhoneNumberSessions ,
                            toUInt8("registrationLendingPromotionsSessions") as registrationLendingPromotionsSessions ,
                            toUInt8("registrationCashbackSessions") as registrationCashbackSessions ,
                            toUInt8("instantDiscountActivationSessions") as instantDiscountActivationSessions ,
                            toUInt8("couponActivationSessions") as couponActivationSessions ,
                            toUInt8("participationInLotterySessions") as participationInLotterySessions ,
                            toUInt8("pagesViews") as pagesViews ,
                            toUInt64("screenView") as screenView ,
                            toUInt8("installApp") as installApp ,
                            toUInt8("installs") as installs ,
                            toString("installationDeviceId") as installationDeviceId ,
                            toString("cityCode") as cityCode ,
                            toUInt32("pageViews") as pageViews ,
                            toString("AppInstallStatHash") as AppInstallStatHash ,
                            toString("AppEventStatHash") as AppEventStatHash ,
                            toString("AppSessionStatHash") as AppSessionStatHash ,
                            toString("AppDeeplinkStatHash") as AppDeeplinkStatHash ,
                            toString("VisitStatHash") as VisitStatHash ,
                            toString("AppMetricaDeviceHash") as AppMetricaDeviceHash ,
                            toString("CrmUserHash") as CrmUserHash ,
                            toString("YmClientHash") as YmClientHash ,
                            toUInt8("__last_click_rank") as __last_click_rank ,
                            toUInt8("__first_click_rank") as __first_click_rank ,
                            toString("__myfirstfunnel_last_click_utmSource") as __myfirstfunnel_last_click_utmSource ,
                            toString("__myfirstfunnel_last_click_utmMedium") as __myfirstfunnel_last_click_utmMedium ,
                            toString("__myfirstfunnel_last_click_utmCampaign") as __myfirstfunnel_last_click_utmCampaign ,
                            toString("__myfirstfunnel_last_click_utmTerm") as __myfirstfunnel_last_click_utmTerm ,
                            toString("__myfirstfunnel_last_click_utmContent") as __myfirstfunnel_last_click_utmContent ,
                            toString("__myfirstfunnel_last_click_adSourceDirty") as __myfirstfunnel_last_click_adSourceDirty ,
                            toString("__myfirstfunnel_first_click_utmSource") as __myfirstfunnel_first_click_utmSource ,
                            toString("__myfirstfunnel_first_click_utmMedium") as __myfirstfunnel_first_click_utmMedium ,
                            toString("__myfirstfunnel_first_click_utmCampaign") as __myfirstfunnel_first_click_utmCampaign ,
                            toString("__myfirstfunnel_first_click_utmTerm") as __myfirstfunnel_first_click_utmTerm ,
                            toString("__myfirstfunnel_first_click_utmContent") as __myfirstfunnel_first_click_utmContent ,
                            toString("__myfirstfunnel_first_click_adSourceDirty") as __myfirstfunnel_first_click_adSourceDirty 

            from test.attr_myfirstfunnel_final_table
        )

        ) 
    WHERE 
    splitByChar('_', __table_name)[4] = 'yd'
    and 
    splitByChar('_', __table_name)[4] = 'testaccount'
    and 
    splitByChar('_', __table_name)[4] = 'default'
    UNION ALL
  
    SELECT * FROM (

        (
            select

                toLowCardinality('full_datestat')  as _dbt_source_relation,
                
                            toString("None") as None ,
                            toDate("__date") as __date ,
                            toString("reportType") as reportType ,
                            toString("accountName") as accountName ,
                            toString("__table_name") as __table_name ,
                            toString("adSourceDirty") as adSourceDirty ,
                            toString("productName") as productName ,
                            toString("adCampaignName") as adCampaignName ,
                            toString("adGroupName") as adGroupName ,
                            toString("adId") as adId ,
                            toString("adPhraseId") as adPhraseId ,
                            toString("utmSource") as utmSource ,
                            toString("utmMedium") as utmMedium ,
                            toString("utmCampaign") as utmCampaign ,
                            toString("utmTerm") as utmTerm ,
                            toString("utmContent") as utmContent ,
                            toString("utmHash") as utmHash ,
                            toString("adTitle1") as adTitle1 ,
                            toString("adTitle2") as adTitle2 ,
                            toString("adText") as adText ,
                            toString("adPhraseName") as adPhraseName ,
                            toFloat64("adCost") as adCost ,
                            toInt32("impressions") as impressions ,
                            toInt32("clicks") as clicks ,
                            toDateTime("__emitted_at") as __emitted_at ,
                            toString("__link") as __link ,
                            toString("AdCostStatHash") as AdCostStatHash ,
                            toString("__id") as __id ,
                            toDateTime("__datetime") as __datetime ,
                            toInt64(0) as __period_number ,
                            toBool(0) as __if_missed ,
                            toUInt16(0) as __priority ,
                            toNullable('') as __step ,
                            toUInt64(0) as qid ,
                            toDateTime(0) as event_datetime ,
                            toString('') as appmetricaDeviceId ,
                            toString('') as mobileAdsId ,
                            toString('') as crmUserId ,
                            toString('') as visitId ,
                            toString('') as clientId ,
                            toString('') as promoCode ,
                            toString('') as osName ,
                            toString('') as cityName ,
                            toString('') as transactionId ,
                            toUInt8(0) as sessions ,
                            toUInt8(0) as addToCartSessions ,
                            toUInt8(0) as cartViewSessions ,
                            toUInt8(0) as checkoutSessions ,
                            toUInt8(0) as webSalesSessions ,
                            toUInt8(0) as sales ,
                            toNullable(0) as amountSales ,
                            toUInt8(0) as registrationCardSessions ,
                            toUInt8(0) as registrationButtonClick ,
                            toUInt8(0) as linkingCardToPhoneNumberSessions ,
                            toUInt8(0) as registrationLendingPromotionsSessions ,
                            toUInt8(0) as registrationCashbackSessions ,
                            toUInt8(0) as instantDiscountActivationSessions ,
                            toUInt8(0) as couponActivationSessions ,
                            toUInt8(0) as participationInLotterySessions ,
                            toUInt8(0) as pagesViews ,
                            toUInt64(0) as screenView ,
                            toUInt8(0) as installApp ,
                            toUInt8(0) as installs ,
                            toString('') as installationDeviceId ,
                            toString('') as cityCode ,
                            toUInt32(0) as pageViews ,
                            toString('') as AppInstallStatHash ,
                            toString('') as AppEventStatHash ,
                            toString('') as AppSessionStatHash ,
                            toString('') as AppDeeplinkStatHash ,
                            toString('') as VisitStatHash ,
                            toString('') as AppMetricaDeviceHash ,
                            toString('') as CrmUserHash ,
                            toString('') as YmClientHash ,
                            toUInt8(0) as __last_click_rank ,
                            toUInt8(0) as __first_click_rank ,
                            toString('') as __myfirstfunnel_last_click_utmSource ,
                            toString('') as __myfirstfunnel_last_click_utmMedium ,
                            toString('') as __myfirstfunnel_last_click_utmCampaign ,
                            toString('') as __myfirstfunnel_last_click_utmTerm ,
                            toString('') as __myfirstfunnel_last_click_utmContent ,
                            toString('') as __myfirstfunnel_last_click_adSourceDirty ,
                            toString('') as __myfirstfunnel_first_click_utmSource ,
                            toString('') as __myfirstfunnel_first_click_utmMedium ,
                            toString('') as __myfirstfunnel_first_click_utmCampaign ,
                            toString('') as __myfirstfunnel_first_click_utmTerm ,
                            toString('') as __myfirstfunnel_first_click_utmContent ,
                            toString('') as __myfirstfunnel_first_click_adSourceDirty 

            from test.full_datestat
        )

        union all
        

        (
            select

                toLowCardinality('attr_myfirstfunnel_final_table')  as _dbt_source_relation,
                
                            toString("None") as None ,
                            toDate("__date") as __date ,
                            toString('') as reportType ,
                            toString("accountName") as accountName ,
                            toString("__table_name") as __table_name ,
                            toString("adSourceDirty") as adSourceDirty ,
                            toString('') as productName ,
                            toString('') as adCampaignName ,
                            toString('') as adGroupName ,
                            toString('') as adId ,
                            toString('') as adPhraseId ,
                            toString("utmSource") as utmSource ,
                            toString("utmMedium") as utmMedium ,
                            toString("utmCampaign") as utmCampaign ,
                            toString("utmTerm") as utmTerm ,
                            toString("utmContent") as utmContent ,
                            toString("utmHash") as utmHash ,
                            toString('') as adTitle1 ,
                            toString('') as adTitle2 ,
                            toString('') as adText ,
                            toString('') as adPhraseName ,
                            toFloat64(0) as adCost ,
                            toInt32(0) as impressions ,
                            toInt32(0) as clicks ,
                            toDateTime("__emitted_at") as __emitted_at ,
                            toString("__link") as __link ,
                            toString('') as AdCostStatHash ,
                            toString("__id") as __id ,
                            toDateTime("__datetime") as __datetime ,
                            toInt64("__period_number") as __period_number ,
                            toBool("__if_missed") as __if_missed ,
                            toUInt16("__priority") as __priority ,
                            toNullable("__step") as __step ,
                            toUInt64("qid") as qid ,
                            toDateTime("event_datetime") as event_datetime ,
                            toString("appmetricaDeviceId") as appmetricaDeviceId ,
                            toString("mobileAdsId") as mobileAdsId ,
                            toString("crmUserId") as crmUserId ,
                            toString("visitId") as visitId ,
                            toString("clientId") as clientId ,
                            toString("promoCode") as promoCode ,
                            toString("osName") as osName ,
                            toString("cityName") as cityName ,
                            toString("transactionId") as transactionId ,
                            toUInt8("sessions") as sessions ,
                            toUInt8("addToCartSessions") as addToCartSessions ,
                            toUInt8("cartViewSessions") as cartViewSessions ,
                            toUInt8("checkoutSessions") as checkoutSessions ,
                            toUInt8("webSalesSessions") as webSalesSessions ,
                            toUInt8("sales") as sales ,
                            toNullable("amountSales") as amountSales ,
                            toUInt8("registrationCardSessions") as registrationCardSessions ,
                            toUInt8("registrationButtonClick") as registrationButtonClick ,
                            toUInt8("linkingCardToPhoneNumberSessions") as linkingCardToPhoneNumberSessions ,
                            toUInt8("registrationLendingPromotionsSessions") as registrationLendingPromotionsSessions ,
                            toUInt8("registrationCashbackSessions") as registrationCashbackSessions ,
                            toUInt8("instantDiscountActivationSessions") as instantDiscountActivationSessions ,
                            toUInt8("couponActivationSessions") as couponActivationSessions ,
                            toUInt8("participationInLotterySessions") as participationInLotterySessions ,
                            toUInt8("pagesViews") as pagesViews ,
                            toUInt64("screenView") as screenView ,
                            toUInt8("installApp") as installApp ,
                            toUInt8("installs") as installs ,
                            toString("installationDeviceId") as installationDeviceId ,
                            toString("cityCode") as cityCode ,
                            toUInt32("pageViews") as pageViews ,
                            toString("AppInstallStatHash") as AppInstallStatHash ,
                            toString("AppEventStatHash") as AppEventStatHash ,
                            toString("AppSessionStatHash") as AppSessionStatHash ,
                            toString("AppDeeplinkStatHash") as AppDeeplinkStatHash ,
                            toString("VisitStatHash") as VisitStatHash ,
                            toString("AppMetricaDeviceHash") as AppMetricaDeviceHash ,
                            toString("CrmUserHash") as CrmUserHash ,
                            toString("YmClientHash") as YmClientHash ,
                            toUInt8("__last_click_rank") as __last_click_rank ,
                            toUInt8("__first_click_rank") as __first_click_rank ,
                            toString("__myfirstfunnel_last_click_utmSource") as __myfirstfunnel_last_click_utmSource ,
                            toString("__myfirstfunnel_last_click_utmMedium") as __myfirstfunnel_last_click_utmMedium ,
                            toString("__myfirstfunnel_last_click_utmCampaign") as __myfirstfunnel_last_click_utmCampaign ,
                            toString("__myfirstfunnel_last_click_utmTerm") as __myfirstfunnel_last_click_utmTerm ,
                            toString("__myfirstfunnel_last_click_utmContent") as __myfirstfunnel_last_click_utmContent ,
                            toString("__myfirstfunnel_last_click_adSourceDirty") as __myfirstfunnel_last_click_adSourceDirty ,
                            toString("__myfirstfunnel_first_click_utmSource") as __myfirstfunnel_first_click_utmSource ,
                            toString("__myfirstfunnel_first_click_utmMedium") as __myfirstfunnel_first_click_utmMedium ,
                            toString("__myfirstfunnel_first_click_utmCampaign") as __myfirstfunnel_first_click_utmCampaign ,
                            toString("__myfirstfunnel_first_click_utmTerm") as __myfirstfunnel_first_click_utmTerm ,
                            toString("__myfirstfunnel_first_click_utmContent") as __myfirstfunnel_first_click_utmContent ,
                            toString("__myfirstfunnel_first_click_adSourceDirty") as __myfirstfunnel_first_click_adSourceDirty 

            from test.attr_myfirstfunnel_final_table
        )

        ) 
    WHERE 
    splitByChar('_', __table_name)[4] = 'appmetrica'
    and 
    splitByChar('_', __table_name)[4] = 'testaccount'
    and 
    splitByChar('_', __table_name)[4] = 'default'
    UNION ALL
  
    SELECT * FROM (

        (
            select

                toLowCardinality('full_datestat')  as _dbt_source_relation,
                
                            toString("None") as None ,
                            toDate("__date") as __date ,
                            toString("reportType") as reportType ,
                            toString("accountName") as accountName ,
                            toString("__table_name") as __table_name ,
                            toString("adSourceDirty") as adSourceDirty ,
                            toString("productName") as productName ,
                            toString("adCampaignName") as adCampaignName ,
                            toString("adGroupName") as adGroupName ,
                            toString("adId") as adId ,
                            toString("adPhraseId") as adPhraseId ,
                            toString("utmSource") as utmSource ,
                            toString("utmMedium") as utmMedium ,
                            toString("utmCampaign") as utmCampaign ,
                            toString("utmTerm") as utmTerm ,
                            toString("utmContent") as utmContent ,
                            toString("utmHash") as utmHash ,
                            toString("adTitle1") as adTitle1 ,
                            toString("adTitle2") as adTitle2 ,
                            toString("adText") as adText ,
                            toString("adPhraseName") as adPhraseName ,
                            toFloat64("adCost") as adCost ,
                            toInt32("impressions") as impressions ,
                            toInt32("clicks") as clicks ,
                            toDateTime("__emitted_at") as __emitted_at ,
                            toString("__link") as __link ,
                            toString("AdCostStatHash") as AdCostStatHash ,
                            toString("__id") as __id ,
                            toDateTime("__datetime") as __datetime ,
                            toInt64(0) as __period_number ,
                            toBool(0) as __if_missed ,
                            toUInt16(0) as __priority ,
                            toNullable('') as __step ,
                            toUInt64(0) as qid ,
                            toDateTime(0) as event_datetime ,
                            toString('') as appmetricaDeviceId ,
                            toString('') as mobileAdsId ,
                            toString('') as crmUserId ,
                            toString('') as visitId ,
                            toString('') as clientId ,
                            toString('') as promoCode ,
                            toString('') as osName ,
                            toString('') as cityName ,
                            toString('') as transactionId ,
                            toUInt8(0) as sessions ,
                            toUInt8(0) as addToCartSessions ,
                            toUInt8(0) as cartViewSessions ,
                            toUInt8(0) as checkoutSessions ,
                            toUInt8(0) as webSalesSessions ,
                            toUInt8(0) as sales ,
                            toNullable(0) as amountSales ,
                            toUInt8(0) as registrationCardSessions ,
                            toUInt8(0) as registrationButtonClick ,
                            toUInt8(0) as linkingCardToPhoneNumberSessions ,
                            toUInt8(0) as registrationLendingPromotionsSessions ,
                            toUInt8(0) as registrationCashbackSessions ,
                            toUInt8(0) as instantDiscountActivationSessions ,
                            toUInt8(0) as couponActivationSessions ,
                            toUInt8(0) as participationInLotterySessions ,
                            toUInt8(0) as pagesViews ,
                            toUInt64(0) as screenView ,
                            toUInt8(0) as installApp ,
                            toUInt8(0) as installs ,
                            toString('') as installationDeviceId ,
                            toString('') as cityCode ,
                            toUInt32(0) as pageViews ,
                            toString('') as AppInstallStatHash ,
                            toString('') as AppEventStatHash ,
                            toString('') as AppSessionStatHash ,
                            toString('') as AppDeeplinkStatHash ,
                            toString('') as VisitStatHash ,
                            toString('') as AppMetricaDeviceHash ,
                            toString('') as CrmUserHash ,
                            toString('') as YmClientHash ,
                            toUInt8(0) as __last_click_rank ,
                            toUInt8(0) as __first_click_rank ,
                            toString('') as __myfirstfunnel_last_click_utmSource ,
                            toString('') as __myfirstfunnel_last_click_utmMedium ,
                            toString('') as __myfirstfunnel_last_click_utmCampaign ,
                            toString('') as __myfirstfunnel_last_click_utmTerm ,
                            toString('') as __myfirstfunnel_last_click_utmContent ,
                            toString('') as __myfirstfunnel_last_click_adSourceDirty ,
                            toString('') as __myfirstfunnel_first_click_utmSource ,
                            toString('') as __myfirstfunnel_first_click_utmMedium ,
                            toString('') as __myfirstfunnel_first_click_utmCampaign ,
                            toString('') as __myfirstfunnel_first_click_utmTerm ,
                            toString('') as __myfirstfunnel_first_click_utmContent ,
                            toString('') as __myfirstfunnel_first_click_adSourceDirty 

            from test.full_datestat
        )

        union all
        

        (
            select

                toLowCardinality('attr_myfirstfunnel_final_table')  as _dbt_source_relation,
                
                            toString("None") as None ,
                            toDate("__date") as __date ,
                            toString('') as reportType ,
                            toString("accountName") as accountName ,
                            toString("__table_name") as __table_name ,
                            toString("adSourceDirty") as adSourceDirty ,
                            toString('') as productName ,
                            toString('') as adCampaignName ,
                            toString('') as adGroupName ,
                            toString('') as adId ,
                            toString('') as adPhraseId ,
                            toString("utmSource") as utmSource ,
                            toString("utmMedium") as utmMedium ,
                            toString("utmCampaign") as utmCampaign ,
                            toString("utmTerm") as utmTerm ,
                            toString("utmContent") as utmContent ,
                            toString("utmHash") as utmHash ,
                            toString('') as adTitle1 ,
                            toString('') as adTitle2 ,
                            toString('') as adText ,
                            toString('') as adPhraseName ,
                            toFloat64(0) as adCost ,
                            toInt32(0) as impressions ,
                            toInt32(0) as clicks ,
                            toDateTime("__emitted_at") as __emitted_at ,
                            toString("__link") as __link ,
                            toString('') as AdCostStatHash ,
                            toString("__id") as __id ,
                            toDateTime("__datetime") as __datetime ,
                            toInt64("__period_number") as __period_number ,
                            toBool("__if_missed") as __if_missed ,
                            toUInt16("__priority") as __priority ,
                            toNullable("__step") as __step ,
                            toUInt64("qid") as qid ,
                            toDateTime("event_datetime") as event_datetime ,
                            toString("appmetricaDeviceId") as appmetricaDeviceId ,
                            toString("mobileAdsId") as mobileAdsId ,
                            toString("crmUserId") as crmUserId ,
                            toString("visitId") as visitId ,
                            toString("clientId") as clientId ,
                            toString("promoCode") as promoCode ,
                            toString("osName") as osName ,
                            toString("cityName") as cityName ,
                            toString("transactionId") as transactionId ,
                            toUInt8("sessions") as sessions ,
                            toUInt8("addToCartSessions") as addToCartSessions ,
                            toUInt8("cartViewSessions") as cartViewSessions ,
                            toUInt8("checkoutSessions") as checkoutSessions ,
                            toUInt8("webSalesSessions") as webSalesSessions ,
                            toUInt8("sales") as sales ,
                            toNullable("amountSales") as amountSales ,
                            toUInt8("registrationCardSessions") as registrationCardSessions ,
                            toUInt8("registrationButtonClick") as registrationButtonClick ,
                            toUInt8("linkingCardToPhoneNumberSessions") as linkingCardToPhoneNumberSessions ,
                            toUInt8("registrationLendingPromotionsSessions") as registrationLendingPromotionsSessions ,
                            toUInt8("registrationCashbackSessions") as registrationCashbackSessions ,
                            toUInt8("instantDiscountActivationSessions") as instantDiscountActivationSessions ,
                            toUInt8("couponActivationSessions") as couponActivationSessions ,
                            toUInt8("participationInLotterySessions") as participationInLotterySessions ,
                            toUInt8("pagesViews") as pagesViews ,
                            toUInt64("screenView") as screenView ,
                            toUInt8("installApp") as installApp ,
                            toUInt8("installs") as installs ,
                            toString("installationDeviceId") as installationDeviceId ,
                            toString("cityCode") as cityCode ,
                            toUInt32("pageViews") as pageViews ,
                            toString("AppInstallStatHash") as AppInstallStatHash ,
                            toString("AppEventStatHash") as AppEventStatHash ,
                            toString("AppSessionStatHash") as AppSessionStatHash ,
                            toString("AppDeeplinkStatHash") as AppDeeplinkStatHash ,
                            toString("VisitStatHash") as VisitStatHash ,
                            toString("AppMetricaDeviceHash") as AppMetricaDeviceHash ,
                            toString("CrmUserHash") as CrmUserHash ,
                            toString("YmClientHash") as YmClientHash ,
                            toUInt8("__last_click_rank") as __last_click_rank ,
                            toUInt8("__first_click_rank") as __first_click_rank ,
                            toString("__myfirstfunnel_last_click_utmSource") as __myfirstfunnel_last_click_utmSource ,
                            toString("__myfirstfunnel_last_click_utmMedium") as __myfirstfunnel_last_click_utmMedium ,
                            toString("__myfirstfunnel_last_click_utmCampaign") as __myfirstfunnel_last_click_utmCampaign ,
                            toString("__myfirstfunnel_last_click_utmTerm") as __myfirstfunnel_last_click_utmTerm ,
                            toString("__myfirstfunnel_last_click_utmContent") as __myfirstfunnel_last_click_utmContent ,
                            toString("__myfirstfunnel_last_click_adSourceDirty") as __myfirstfunnel_last_click_adSourceDirty ,
                            toString("__myfirstfunnel_first_click_utmSource") as __myfirstfunnel_first_click_utmSource ,
                            toString("__myfirstfunnel_first_click_utmMedium") as __myfirstfunnel_first_click_utmMedium ,
                            toString("__myfirstfunnel_first_click_utmCampaign") as __myfirstfunnel_first_click_utmCampaign ,
                            toString("__myfirstfunnel_first_click_utmTerm") as __myfirstfunnel_first_click_utmTerm ,
                            toString("__myfirstfunnel_first_click_utmContent") as __myfirstfunnel_first_click_utmContent ,
                            toString("__myfirstfunnel_first_click_adSourceDirty") as __myfirstfunnel_first_click_adSourceDirty 

            from test.attr_myfirstfunnel_final_table
        )

        ) 
    WHERE 
    splitByChar('_', __table_name)[4] = 'ym'
    and 
    splitByChar('_', __table_name)[4] = 'testaccount'
    and 
    splitByChar('_', __table_name)[4] = 'default'
  