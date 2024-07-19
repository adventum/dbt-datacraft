-- depends_on: test.hash_events
SELECT __date,__table_name,event_datetime,accountName,appmetricaDeviceId,mobileAdsId,crmUserId,promoCode,osName,cityName,adSourceDirty,utmSource,utmMedium,utmCampaign,utmTerm,utmContent,transactionId,utmHash,SUM(sessions) AS sessions,SUM(addToCartSessions) AS addToCartSessions,SUM(cartViewSessions) AS cartViewSessions,SUM(checkoutSessions) AS checkoutSessions,SUM(webSalesSessions) AS webSalesSessions,SUM(sales) AS sales,SUM(amountSales) AS amountSales,SUM(registrationCardSessions) AS registrationCardSessions,SUM(registrationButtonClick) AS registrationButtonClick,SUM(linkingCardToPhoneNumberSessions) AS linkingCardToPhoneNumberSessions,SUM(registrationLendingPromotionsSessions) AS registrationLendingPromotionsSessions,SUM(registrationCashbackSessions) AS registrationCashbackSessions,SUM(instantDiscountActivationSessions) AS instantDiscountActivationSessions,SUM(couponActivationSessions) AS couponActivationSessions,SUM(participationInLotterySessions) AS participationInLotterySessions,SUM(pagesViews) AS pagesViews,SUM(screenView) AS screenView,SUM(installApp) AS installApp,SUM(installs) AS installs,installationDeviceId,__emitted_at,__link,touch_type,touch_time,install_time,event_time,event_name,event_source,partner,media_source,campaign,site_id,ad,country_code,city,appsflyer_id,custom_uid,platform,is_retargeting,is_primary_attribution,visitId,clientId,cityCode,SUM(pageViews) AS pageViews,VisitStatHash,AppInstallStatHash,AppEventStatHash,AppSessionStatHash,AppDeeplinkStatHash,YmClientHash,UtmHashHash,AppMetricaDeviceHash,CrmUserHash,__id,__datetime 
FROM test.hash_events
GROUP BY __date, __table_name, event_datetime, accountName, appmetricaDeviceId, mobileAdsId, crmUserId, promoCode, osName, cityName, adSourceDirty, utmSource, utmMedium, utmCampaign, utmTerm, utmContent, transactionId, utmHash, installationDeviceId, __emitted_at, __link, touch_type, touch_time, install_time, event_time, event_name, event_source, partner, media_source, campaign, site_id, ad, country_code, city, appsflyer_id, custom_uid, platform, is_retargeting, is_primary_attribution, visitId, clientId, cityCode, VisitStatHash, AppInstallStatHash, AppEventStatHash, AppSessionStatHash, AppDeeplinkStatHash, YmClientHash, UtmHashHash, AppMetricaDeviceHash, CrmUserHash, __id, __datetime
