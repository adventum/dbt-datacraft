


WITH ad_plans_statistics AS (
SELECT * FROM test.incremental_vkads_default_ad_plans_statistics 
WHERE toDate(__datetime) between '2023-12-20' and '2024-01-18'),

ad_plans AS (
SELECT * FROM test.incremental_vkads_default_ad_plans
)

SELECT
    toDate(ad_plans_statistics.__datetime) AS __datetime,
    '' AS reportType,
    toLowCardinality(splitByChar('_', ad_plans.__table_name)[6]) AS accountName,
    ad_plans.__table_name AS __table_name,
    'VK Ads' AS adSourceDirty,
    '' AS productName,
    ad_plans.name AS adCampaignName,
    '' AS adGroupName,
    ad_plans.id AS adId,
    '' AS adPhraseId,
    '' AS utmSource,
    '' AS utmMedium,
    '' AS utmCampaign,
    '' AS utmTerm,
    '' AS utmContent,
    '' AS adTitle1,
    '' AS adTitle2,
    '' AS adText,
    '' AS adPhraseName,  
    toFloat64(JSONExtractString(ad_plans_statistics.base, 'spent')) AS adCost,
    toInt32(JSONExtractString(ad_plans_statistics.base, 'shows')) AS impressions,
    toInt32(JSONExtractString(ad_plans_statistics.base, 'clicks')) AS clicks
FROM ad_plans
JOIN ad_plans_statistics ON ad_plans.id = ad_plans_statistics.ad_plan_id








