

  create view test.link_events__dbt_tmp 
  
  as (
    -- depends_on: test.hash_events
SELECT _dbt_source_relation,None,__date,__table_name,event_datetime,accountName,appmetricaDeviceId,mobileAdsId,crmUserId,visitId,clientId,promoCode,osName,cityName,adSourceDirty,utmSource,utmMedium,utmCampaign,utmTerm,utmContent,transactionId,utmHash,sessions,addToCartSessions,cartViewSessions,checkoutSessions,webSalesSessions,sales,amountSales,registrationCardSessions,registrationButtonClick,linkingCardToPhoneNumberSessions,registrationLendingPromotionsSessions,registrationCashbackSessions,instantDiscountActivationSessions,couponActivationSessions,participationInLotterySessions,pagesViews,screenView,installApp,installs,installationDeviceId,__emitted_at,__link,cityCode,pageViews,AppInstallStatHash,AppEventStatHash,AppSessionStatHash,AppDeeplinkStatHash,VisitStatHash,AppMetricaDeviceHash,CrmUserHash,YmClientHash,__id,__datetime 
FROM test.hash_events
GROUP BY _dbt_source_relation, None, __date, __table_name, event_datetime, accountName, appmetricaDeviceId, mobileAdsId, crmUserId, visitId, clientId, promoCode, osName, cityName, adSourceDirty, utmSource, utmMedium, utmCampaign, utmTerm, utmContent, transactionId, utmHash, sessions, addToCartSessions, cartViewSessions, checkoutSessions, webSalesSessions, sales, amountSales, registrationCardSessions, registrationButtonClick, linkingCardToPhoneNumberSessions, registrationLendingPromotionsSessions, registrationCashbackSessions, instantDiscountActivationSessions, couponActivationSessions, participationInLotterySessions, pagesViews, screenView, installApp, installs, installationDeviceId, __emitted_at, __link, cityCode, pageViews, AppInstallStatHash, AppEventStatHash, AppSessionStatHash, AppDeeplinkStatHash, VisitStatHash, AppMetricaDeviceHash, CrmUserHash, YmClientHash, __id, __datetime


  )