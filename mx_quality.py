import pandas as pd
import pymysql.cursors
import numpy as np
from google.oauth2 import service_account
import pandas_gbq
import os
import datetime
from google.cloud import bigquery
import pytz


def fetch_aryan_password():
    if "ARYAN_MAD_PASSWORD" not in os.environ.keys():
        raise ValueError("env var ARYAN_MAD_PASSWORD not defined")
    return os.environ.get("ARYAN_MAD_PASSWORD")


yesterday_date = datetime.datetime.now(pytz.timezone('Asia/Kolkata')) - datetime.timedelta(days=0)

formatted_yesterday = yesterday_date.strftime("%Y-%m-%d")

pd.set_option("display.max_columns", 100)

HOME_DIR = '/home/'

bq_auth_file = HOME_DIR + 'magicpin-analytics-tactical.json'

project_id_bq = 'magicpin-analytics'
credentials_bq = service_account.Credentials.from_service_account_file(bq_auth_file)
client_bq = bigquery.Client(credentials=credentials_bq, project=project_id_bq)


def task1(date):
    print('Delete Function Imported')
    query = "delete  FROM `magicpin-analytics.locality_frame.quality_mx` where date(updated_at) = '" + str(date) + "';"
    query_job = client_bq.query(query)
    return query_job.result()


def categorize_locality(row):
    if row['cluster_locality_id'] in [1547, 1543, 4120, 686, 715, 1545, 1379, 548, 1381, 1851, 1550, 551, 683, 1391,
                                      1561, 11487, 1546, 1568, 11343, 1551, 1378, 1377, 1398, 1402, 1392, 1558, 1376,
                                      1542, 1512, 1416, 3808, 724, 2991, 1562, 10674, 1414, 557, 1407, 17357, 11096,
                                      567, 553, 1549, 1557, 4219, 725, 554, 1385, 1401, 1371, 1846, 565, 1404, 1383,
                                      11329, 31650, 1544, 1380, 1394, 1564, 1372, 556, 2866, 1389]:
        return 'New Localities'
    elif row['cluster_locality_id'] in [628, 1187, 604, 14239, 521, 511, 757, 502, 606, 683, 765, 4139, 632, 1205, 16,
                                      603, 15925, 776, 778, 20, 621, 495, 489, 28, 31, 493, 610, 491, 35, 546, 794, 795,
                                      38, 619, 2804, 657, 894, 799, 1172, 488, 625, 427, 565, 1170, 45, 1258, 497, 49,
                                      46193, 46148, 10557, 15962, 653, 57, 58, 60, 524, 527, 62, 14591, 1166, 63, 641,
                                      12565, 74, 11192, 418, 618, 540, 10526, 835, 405, 93, 893, 1380, 633, 644, 541,
                                      416, 605, 650, 9, 1190, 780, 630, 510, 612, 1192, 1251, 51, 59, 1215, 830, 11106,
                                      1161, 82, 87, 1252, 499]:
        return 'Top 100 Localities'
    elif row['cluster_locality_id'] in [1787, 2352, 762, 748, 1203, 1636, 11487, 1213, 659, 753, 577, 613, 2155, 755,
                                        14270, 498, 1372, 1212, 631, 766, 2954, 13358, 773, 601, 4120, 496, 602, 2930,
                                        46168, 2365, 1542, 781, 14003, 23, 1544, 532, 1188, 1637, 784, 793, 638, 528,
                                        1244, 1201, 635, 895, 2030, 801, 4076, 15927, 803, 14459, 634, 874, 2935, 12358,
                                        14109, 807, 808, 567, 1214, 814, 1238, 4254, 1377, 896, 490, 31572, 1242, 12478,
                                        262, 826, 56, 31650, 651, 622, 11485, 551, 1899, 500, 64, 522, 647, 508, 10523,
                                        624, 891, 306, 77, 515, 616, 655, 1702, 14661, 662, 548, 407, 1271, 514, 660,
                                        519]:
        return 'Top 100-200 Localities'
    elif row['cluster_locality_id'] in [2905, 1164, 15920, 888, 2353, 513, 752, 13719, 1169, 1691, 512, 1644, 17367,
                                        1700, 754, 4370, 2499, 556, 889, 763, 11612, 13543, 1943, 770, 10, 4219, 669,
                                        10496, 1682, 15949, 2890, 764, 184, 11329, 10666, 26, 3808, 3837, 10674, 1249,
                                        1175, 1689, 203, 1676, 509, 1697, 797, 1194, 3038, 11569, 591, 557, 1677, 507,
                                        1218, 2908, 640, 1896, 50, 11581, 2489, 14501, 1670, 1207, 430, 823, 1690,
                                        46199, 1653, 1307, 828, 829, 271, 61, 1304, 1612, 2914, 1294, 13372, 1189, 2915,
                                        46162, 15976, 1224, 1706, 70, 301, 302, 11826, 312, 456, 1634, 362, 1714, 14654,
                                        2918, 18122, 1657, 1237, 92, 94, 460, 1562, 847, 95, 15945, 652, 2553, 614,
                                        15752, 23765, 851, 1239, 31627]:
        return 'High Potential Locality'
    else:
        return 'others'


def upload_to_bq(df):
    task1(formatted_yesterday)
    pd.set_option("display.max_columns", 100)
    project_id_bq = 'magicpin-analytics'
    credentials = service_account.Credentials.from_service_account_file(bq_auth_file)
    pandas_gbq.to_gbq(df, 'locality_frame.quality_mx', project_id_bq, chunksize=90000, credentials=credentials,
                      if_exists='append')


def runner_maddb(sql):
    with pymysql.connect(host='mad.gc.magicpin.in', user='analytics', password=fetch_aryan_password(),
                         db='aryan') as conn:
        cur = conn.cursor()
        cur.execute(sql)
        cdata = cur.fetchall()
        names = [x[0] for x in cur.description]
        df = pd.DataFrame(cdata, columns=names)
        return df


credentials = service_account.Credentials.from_service_account_file(bq_auth_file)


def df_from_bq(query):
    rdf = pandas_gbq.read_gbq(query, project_id='magicpin-14cba', credentials=credentials)
    rdf = pd.DataFrame(rdf)
    return rdf


all_merchants = """SELECT distinct *except(monthly_sales,res_chain_url) FROM `magicpin-analytics.locality_frame.quality_merchants_fixed_new`;"""

all_merchants = df_from_bq(all_merchants)

toggle_on = """SELECT
  DISTINCT actorId AS mid,
  LOWER(updatedValue) AS toggle_on
FROM
  `magicpin-14cba.delivery_as.accepting_delivery_logs`
WHERE
  timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 52 HOUR)
  AND UPPER(updatedValue) = 'TRUE';"""

toggle_on = df_from_bq(toggle_on)

all_merchants['mid'] = all_merchants['mid'].astype(int)

toggle_on['mid'] = toggle_on['mid'].astype(int)

all_merchants = all_merchants.merge(toggle_on, how='left', on='mid')

all_merchants = all_merchants.drop_duplicates()

mfd_data = """SELECT
  distinct m.id AS mid,
  package_type,
  ifnull(pc.commission_value,
    delivery_commission) AS commision_value,
  pc.delivery_commission AS delivery_commission,
  merchant_sponsored_usable_balance_percent AS mfd,
  merchant_sponsored_discount_commission AS mfd_c,
  (ifnull(pc.merchant_sponsored_usable_balance_percent,
      0) + pc.magicpay_usable_balance_percent) AS savepercent,
  pc.is_shadowed AS is_listed,
  ifnull(CAST(SPLIT(SPLIT(magicorder_day_wise_mfd_commission,",")[safe_OFFSET(1)],":")[safe_OFFSET(1)] AS float64),
    merchant_sponsored_discount_commission) AS magicorder_mfd,
  ifnull(CAST(SPLIT(SPLIT(magicpay_day_wise_mfd_commission,",")[safe_OFFSET(1)],":")[safe_OFFSET(1)] AS float64),
    merchant_sponsored_discount_commission) AS magicpay_mfd,
  ifnull(CAST(SPLIT(SPLIT(cash_voucher_day_wise_mfd_commission,",")[safe_OFFSET(1)],":")[safe_OFFSET(1)] AS float64),
    merchant_sponsored_discount_commission) AS cashvoucher_mfd,
  ifnull(CAST(SPLIT(SPLIT(combo_voucher_day_wise_mfd_commission,",")[safe_OFFSET(1)],":")[safe_OFFSET(1)] AS float64),
    merchant_sponsored_discount_commission) AS combovoucher_mfd,
  ifnull(CAST(SPLIT(SPLIT(magicorder_day_wise_mfd,",")[safe_OFFSET(1)],":")[safe_OFFSET(1)] AS float64),
    merchant_sponsored_usable_balance_percent) + magicorder_usable_balance_percent AS delivery_save,
  ifnull(CAST(SPLIT(SPLIT(magicpay_day_wise_mfd,",")[safe_OFFSET(1)],":")[safe_OFFSET(1)] AS float64),
    merchant_sponsored_discount_commission) + magicpay_usable_balance_percent AS magicpay_save,
  ifnull(CAST(SPLIT(SPLIT(cash_voucher_day_wise_mfd,",")[safe_OFFSET(1)],":")[safe_OFFSET(1)] AS float64),
    merchant_sponsored_discount_commission) + redemption_usable_balance_percent AS voucher_save
FROM
  `magicpin-14cba.wallet_latest.payment_commercials` pc
JOIN
  `magicpin-14cba.wallet_latest.merchant` m
ON
  m.user_id = pc.merchant_user_id
WHERE
  LOWER(pc.commercials_type) = 'marketing'
  and ifnull(pc.is_active,1)= 1
  and ifnull(pc.is_shadowed,0) = 0
  AND m.country IN ('India')
  ;"""

mfd_data = df_from_bq(mfd_data)

mfd_data['mid'] = mfd_data['mid'].astype(int)

all_merchants = all_merchants.merge(mfd_data, how='left', on='mid')

all_merchants = all_merchants.drop_duplicates()

voucher_listed = """
SELECT
  pid as PID,
  mid,
  case when loweR(ifnull(magicpay_status,"na")) = 'magicpay in on' then 1 else 0 end as magicpay_status,
  ifnull(enable_order_booking,0) as magic_order_status,
  PID_CHECK,
  case when loweR(ifnull(Denomination_status,"na")) = 'denomination available' then 1 else 0 end as voucher_status
FROM (
  SELECT
    x.*,
    CASE
      WHEN x.voucher_count > 0 THEN 'Denomination available'
    ELSE
    'Denomination not available'
  END
    AS Denomination_status
  FROM (
    SELECT
      m.parent_merchant_id AS PID,
      m.id AS MID,
      m.enable_order_booking,
      m.accepting_delivery,
      m.city,
      m.locality,
      m.merchant_name AS Mx_Name,
      m.user_id AS Mx_User_Id,
      c.name,
      m.highlight1,
      CASE
        WHEN ROTags = "XYZ" THEN 0
        WHEN ROTags >= CAST((CURRENT_DATE() - 15) AS string) THEN 1
      ELSE
      0
    END
      AS ROTags,
      CASE
        WHEN DATE(x.activation_date) IS NULL THEN 0
        WHEN DATE(x.activation_date) >= CURRENT_DATE() - 15 THEN 1
      ELSE
      0
    END
      AS activation_date,
      CASE
        WHEN LEFT(pc.remarks,4) IN ('2019', '2020', '2021', '2022', '2023', '2024') THEN (CASE
          WHEN pc.remarks >= CAST((CURRENT_DATE() - 15) AS string) THEN 1
        ELSE
        0
      END
        )
      ELSE
      0
    END
      AS pc_remarks,
      CASE
        WHEN DATE(pc.unshadow_date) IS NULL THEN 0
        WHEN DATE(pc.unshadow_date) >= CURRENT_DATE() - 15 THEN 1
      ELSE
      0
    END
      AS pc_unshadow_date,
      CASE
        WHEN DATE(pc.last_confirmation_date) IS NULL THEN 0
        WHEN DATE(pc.last_confirmation_date) >= CURRENT_DATE() - 15 THEN 1
      ELSE
      0
    END
      AS last_confirm,
      CASE
        WHEN m.highlight5 ='parent' THEN 1
      ELSE
      0
    END
      AS PID_CHECK,
      CURRENT_DATE() AS Tos_Date,
      CASE
        WHEN DATE_DIFF(CURRENT_DATE(),IFNULL(DATE(pc.last_confirmation_date),'2020-01-01'),day) < 60 THEN 'Call as magicpin'
      ELSE
      'Call as magicpin'
    END
      AS Type_Of_Calling,
      CASE
        WHEN magicpay_app_shadowed=0 THEN 'magicpay in ON'
      ELSE
      'magicpay is OFF'
    END
      AS magicpay_status,
      COUNT(DISTINCT(CASE
            WHEN roa.is_active = 1 AND roa.is_app_shadowed = 0 AND (roa.is_hidden = 0 OR roa.is_hidden IS NULL) THEN roa.id
        END
          )) AS voucher_count
    FROM (
      SELECT
        supportedMerchants AS userid,
        updated_at AS UR,
        id AS ROID,
        is_shadowed,
        CASE
          WHEN LEFT(tags, 4) IN ('2019', '2020', '2021', '2022', '2023', '2024') THEN tags
        ELSE
        'XYZ'
      END
        AS ROTags,
        voucher_country AS country1,
        category_tags,
        supportedmerchants,
        activation_date,
        deactivation_date,
        is_active
      FROM (
        SELECT
          * EXCEPT(c) REPLACE(c AS supportedMerchants)
        FROM
          `magicpin-14cba.wallet_latest.redemption_option`,
          UNNEST(SPLIT(supportedMerchants))c
        WHERE
          supportedMerchants IS NOT NULL
          AND is_active = 1
          AND supportedMerchants != '')x
      GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11
      UNION ALL
      SELECT
        DISTINCT merchantuserid,
        ro.updated_at AS UR,
        id AS ROID,
        is_shadowed,
        CASE
          WHEN LEFT(tags, 4) IN ('2019', '2020', '2021', '2022', '2023', '2024') THEN tags
        ELSE
        'XYZ'
      END
        AS ROTags,
        voucher_country AS country1,
        category_tags,
        supportedmerchants,
        activation_date,
        deactivation_date,
        is_active
      FROM
        `magicpin-14cba.wallet_latest.redemption_option` ro
      WHERE
        is_active = 1
        AND merchantuserid IS NOT NULL )x
    LEFT JOIN
      `magicpin-14cba.wallet_latest.merchant` m
    ON
      x.userid = CAST(m.user_id AS string)
    LEFT JOIN
      `magicpin-14cba.wallet_latest.redemption_option_amount` roa
    ON
      roa.option_id=x.ROID
    LEFT JOIN
      `magicpin-14cba.wallet_latest.redemption_option_partner` rop
    ON
      rop.option_amount_id = roa.id
    LEFT JOIN
      `magicpin-14cba.wallet_latest.payment_commercials` pc
    ON
      CAST(pc.merchant_user_id AS string) = x.userid
    LEFT JOIN
      `magicpin-14cba.wallet_latest.category` c
    ON
      m.category_id = c.id
    WHERE
      m.id IS NOT NULL
      AND m.is_shadowed=0
      AND m.city NOT IN ('Bekasi',
        'Jakarta',
        'Depok',
        'Tangerang')
      AND pc.is_shadowed = 0
      AND pc.commercials_type = 'MARKETING'
      AND m.category_id IN (1,
        2,
        3,
        4,
        5,
        91,
        58,
        99)
      AND LOWER(campaign_tags) NOT LIKE "%ondc_non%"
      AND LOWER(m.campaign_tags) NOT LIKE "%ondc_non_magicpin%"
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18,
      19
    HAVING
      PID_CHECK = 0)x);"""

voucher_listed = df_from_bq(voucher_listed)

voucher_listed['mid'] = voucher_listed['mid'].astype(int)

all_merchants = all_merchants.merge(voucher_listed, how='left', on='mid')

all_merchants = all_merchants.drop_duplicates()

mids = tuple(all_merchants['mid'].tolist())

mx_commercial = """select distinct m.id as mid, pc.is_shadowed as merchant_commercial
  from wallet_latest.merchant m
  left join wallet_latest.payment_commercials pc  on pc.merchant_user_id = m.user_id
  WHERE LOWER(pc.commercials_type) in  ('marketing') AND m.country IN ('India')
  and pc.is_shadowed  = 0
  and ifnull(pc.is_active,1) = 1
  and m.id in (SELECT distinct mid FROM `magicpin-analytics.locality_frame.quality_merchants_fixed_new`)
  ;"""

mx_commercial = df_from_bq(mx_commercial)

mx_commercial['mid'] = mx_commercial['mid'].astype(int)

all_merchants = all_merchants.merge(mx_commercial, how='left', on='mid')

all_merchants = all_merchants.drop_duplicates()

all_merchants['merchant_commercial'] = all_merchants['merchant_commercial'].fillna(1)

category_data = """SELECT
  m.id AS mid,
  (CASE
WHEN m.category_id = 1 and (lower(ifnull(highlight1,"na")) in ('fine dining','nightlife','luxury dining') OR lower(ifnull(highlight1,"na") IN ('fine dining','nightlife','luxury dining'))) then 'Fine Dine'
WHEN m.category_id  = 1 and (lower(highlight1) in ('delivery only') or lower(highlight3) in ('delivery only')) then 'delivery only'
WHEN m.category_id = 1 THEN 'QSR'
end) as Sub_Category
,lower(highlight5)  as highlight5
FROM
  aryan.merchant m

where m.id in """ + str(mids) + """;"""

category_data = runner_maddb(category_data)

category_data['mid'] = category_data['mid'].astype(int)

all_merchants = all_merchants.merge(category_data, how='left', on='mid')

all_merchants = all_merchants.drop_duplicates()

all_merchants = all_merchants.fillna(0)

all_merchants['voucher_status'] = np.where(((all_merchants['voucher_status'].fillna(0) > 0)), 1, 0)

all_merchants['online_status'] = np.where(
    ((all_merchants['magic_order_status'] > 0) & (all_merchants['merchant_commercial'] == 0)), 1, 0)

all_merchants['magicorer_toggle_on'] = np.where(
    ((all_merchants['toggle_on'] == "true") & (all_merchants['merchant_commercial'] == 0)), 1, 0)
all_merchants['online_toggle_on'] = np.where(
    (all_merchants['magicorer_toggle_on'] == 1), 1, 0)

all_merchants['offline'] = all_merchants['magicpay_status'] + all_merchants['voucher_status']
all_merchants['offline_status'] = np.where(
    ((all_merchants['offline'] > 0) & (all_merchants['merchant_commercial'] == 0)), 1, 0)

all_merchants['offline_state'] = np.where(((all_merchants['voucher_status'] > 0) & (
        all_merchants['magicpay_status'] > 0) & (all_merchants['merchant_commercial'] == 0)), 1, 0)

all_merchants['adjusted_anyone_listed'] = all_merchants['voucher_status'] + all_merchants['online_toggle_on'] + \
                                          all_merchants['magicpay_status']

condition = [(all_merchants['Sub_Category'] == 'delivery only') & (all_merchants['online_status'] == 1) & (
        all_merchants['toggle_on'] == 'true') & (all_merchants['highlight5'] != 'parent'),
             (all_merchants['Sub_Category'] == 'delivery only') & (all_merchants['online_status'] == 1) & (
                     all_merchants['toggle_on'] != 'true') & (all_merchants['highlight5'] != 'parent'),
             (all_merchants['Sub_Category'] == 'QSR') & (
                     (all_merchants['online_status'] == 1) & (all_merchants['toggle_on'] == 'true') & (
                     all_merchants['highlight5'] != 'parent') | (all_merchants['offline_state'] == 1)),
             (all_merchants['Sub_Category'] == 'QSR') & (all_merchants['online_status'] == 1) & (
                     all_merchants['toggle_on'] != 'true') & (all_merchants['highlight5'] != 'parent'),
             (all_merchants['Sub_Category'] == 'QSR') & (all_merchants['online_status'] == 0) & (
                     all_merchants['offline_state'] == 1) & (all_merchants['highlight5'] != 'parent'),
             (all_merchants['Sub_Category'] == 'QSR') & (all_merchants['online_status'] == 0) & (
                     all_merchants['offline_state'] == 0) & (all_merchants['highlight5'] != 'parent')]

choice = [1, 0, all_merchants['adjusted_anyone_listed'], 0, all_merchants['adjusted_anyone_listed'], 0]

all_merchants['adjusted_anyone_listed'] = np.select(condition, choice, default=all_merchants['adjusted_anyone_listed'])

funded_mx_query = """



SELECT 
    m.id as mid ,
    m.user_id,
    m.merchant_name,
    m.locality,
    m.city,
    mc.start_date,
    mc.end_date,
    mc.is_cashback_non_funded,
    CASE
        WHEN m.category_id = 6 THEN 'online'
        ELSE 'offline'
    END AS merchant_type
FROM
    aryan.merchant_commercials mc
        LEFT JOIN
    cbs.package_subscription ps ON ps.id = mc.package_subscription_id
        LEFT JOIN
    cbs.package p ON ps.package_id = p.id
        LEFT JOIN
    aryan.merchant_commercials_rules mcr ON mc.id = mcr.merchant_commercials_id
        LEFT JOIN
    aryan.merchant m ON m.id = mc.merchant_id
        LEFT JOIN
    aryan.category c ON m.category_id = c.id
WHERE
    CURRENT_DATE() BETWEEN DATE(mc.start_date) AND DATE(mc.end_date)
        AND mc.merchant_commercials_type_id = 1
        AND (merchant_priority = 0 or merchant_priority is null)
        AND mc.merchant_commercials_type_id = 1
        #AND m.category_id = 6
        AND merchant_commercials_type_id = 1
        #AND p.name IN ('Brand' , 'BRAND_CAC', 'VoucherOnlyBrands')
        AND m.country = 'india' 
UNION ALL SELECT 
    m.id,
    m.user_id,
    m.merchant_name,
    m.locality,
    m.city,
    mc.start_date,
    mc.end_date,
    mc.is_cashback_non_funded,
    CASE
        WHEN m.category_id = 6 THEN 'online'
        ELSE 'offline'
    END AS merchant_type
FROM
    merchant_commercials mc
        INNER JOIN
    (SELECT 
        MAX(mc.id) max
    FROM
        merchant_commercials mc
    LEFT JOIN merchant m ON mc.merchant_id = m.id
    WHERE
        parent_merchant_id IN (SELECT 
                m.id
            FROM
                aryan.merchant_commercials mc
            LEFT JOIN cbs.package_subscription ps ON ps.id = mc.package_subscription_id
            LEFT JOIN cbs.package p ON ps.package_id = p.id
            LEFT JOIN aryan.merchant_commercials_rules mcr ON mc.id = mcr.merchant_commercials_id
            LEFT JOIN aryan.merchant m ON m.id = mc.merchant_id
            LEFT JOIN aryan.category c ON m.category_id = c.id
            WHERE
                CURRENT_DATE() BETWEEN DATE(mc.start_date) AND DATE(mc.end_date)
                    AND mc.merchant_commercials_type_id = 1
                    AND (merchant_priority = 0 or merchant_priority is null)
                    AND mc.merchant_commercials_type_id = 1
                    AND m.category_id <> 6
                    AND m.highlight5 = 'parent'
                    AND merchant_commercials_type_id = 1
                    #AND p.name IN ('Brand' , 'BRAND_CAC', 'VoucherOnlyBrands')
                    AND m.country = 'india')
            AND merchant_commercials_type_id = 1
    GROUP BY merchant_id) max_mc ON mc.id = max_mc.max
        LEFT JOIN
    merchant m ON mc.merchant_id = m.id
        LEFT JOIN
    cbs.package_subscription ps ON ps.id = mc.package_subscription_id
        LEFT JOIN
    cbs.package p ON ps.package_id = p.id

    union all

    SELECT 
    m.id,
    m.user_id,
    m.merchant_name,
    m.locality,
    m.city,
    mc.start_date,
    mc.end_date,
    mc.is_cashback_non_funded,
    CASE
        WHEN m.category_id = 6 THEN 'online'
        ELSE 'offline'
    END AS merchant_type
FROM
    aryan.merchant_commercials mc
        LEFT JOIN
    cbs.package_subscription ps ON ps.id = mc.package_subscription_id
        LEFT JOIN
    cbs.package p ON ps.package_id = p.id
        LEFT JOIN
    aryan.merchant_commercials_rules mcr ON mc.id = mcr.merchant_commercials_id
        LEFT JOIN
    aryan.merchant m ON m.id = mc.merchant_id
        LEFT JOIN
    aryan.category c ON m.category_id = c.id
WHERE
    CURRENT_DATE() BETWEEN DATE(mc.start_date) AND DATE(mc.end_date)
        AND mc.merchant_commercials_type_id = 1
        AND merchant_priority = 1
        AND mc.merchant_commercials_type_id = 1
        AND m.category_id = 6
        AND merchant_commercials_type_id = 1
        AND p.name IN ('Brand' , 'BRAND_CAC', 'VoucherOnlyBrands')
        AND m.country = 'india' 
UNION ALL SELECT 
    m.id,
    m.user_id,
    m.merchant_name,
    m.locality,
    m.city,
    mc.start_date,
    mc.end_date,
    mc.is_cashback_non_funded,
    CASE
        WHEN m.category_id = 6 THEN 'online'
        ELSE 'offline'
    END AS merchant_type
FROM
    merchant_commercials mc
        INNER JOIN
    (SELECT 
        MAX(mc.id) max
    FROM
        merchant_commercials mc
    LEFT JOIN merchant m ON mc.merchant_id = m.id
    WHERE
        parent_merchant_id IN (SELECT 
                m.id
            FROM
                aryan.merchant_commercials mc
            LEFT JOIN cbs.package_subscription ps ON ps.id = mc.package_subscription_id
            LEFT JOIN cbs.package p ON ps.package_id = p.id
            LEFT JOIN aryan.merchant_commercials_rules mcr ON mc.id = mcr.merchant_commercials_id
            LEFT JOIN aryan.merchant m ON m.id = mc.merchant_id
            LEFT JOIN aryan.category c ON m.category_id = c.id
            WHERE
                CURRENT_DATE() BETWEEN DATE(mc.start_date) AND DATE(mc.end_date)
                    AND mc.merchant_commercials_type_id = 1
                    AND merchant_priority = 1
                    AND mc.merchant_commercials_type_id = 1
                    AND m.category_id <> 6
                    AND m.highlight5 = 'parent'
                    AND merchant_commercials_type_id = 1
                    AND p.name IN ('Brand' , 'BRAND_CAC', 'VoucherOnlyBrands')
                    AND m.country = 'india')
            AND merchant_commercials_type_id = 1
    GROUP BY merchant_id) max_mc ON mc.id = max_mc.max
        LEFT JOIN
    merchant m ON mc.merchant_id = m.id
        LEFT JOIN
    cbs.package_subscription ps ON ps.id = mc.package_subscription_id
        LEFT JOIN
    cbs.package p ON ps.package_id = p.id"""

funded_mx_query = runner_maddb(funded_mx_query)

funded_mx_query['is_funded'] = 'Yes'

funded_mx_query = funded_mx_query[['mid', 'is_funded']]

funded_mx_query['mid'] = funded_mx_query['mid'].astype(int)

all_merchants = all_merchants.merge(funded_mx_query, how='left', on='mid')

all_merchants = all_merchants.drop_duplicates()

all_merchants['updated_at'] = formatted_yesterday

all_merchants['updated_at'] = pd.to_datetime(all_merchants['updated_at'])

revenue = """SELECT
  atd.merchant_id as mid,
  COUNT(DISTINCT
    CASE
      WHEN LOWER(tlc.product_type) IN ('delivery') and date(rpc_redemption_datetime) = '""" + str(formatted_yesterday) + """' THEN tlc.order_id
  END
    ) AS delivery_txns,
  COUNT(DISTINCT
    CASE
      WHEN LOWER(tlc.product_type) IN ('voucher', 'magicpay', 'epay', 'resarvation') and date(rpc_redemption_datetime) = '""" + str(
    formatted_yesterday) + """' THEN tlc.txn_id
  END
    ) AS offline_transactions,
  SUM(CASE
      WHEN LOWER(tlc.product_type) IN ('delivery') and date(rpc_redemption_datetime) = '""" + str(formatted_yesterday) + """' THEN IFNULL(tlc.revenue,0)+IFNULL(ondc_incentive,0)
  END
    ) AS delivery_txns_revenue,
  SUM(CASE
      WHEN LOWER(tlc.product_type) IN ('voucher', 'magicpay', 'epay', 'resarvation') and date(rpc_redemption_datetime) = '""" + str(
    formatted_yesterday) + """' THEN IFNULL(tlc.revenue,0)+IFNULL(ondc_incentive,0)
  END
    ) AS offline_transactions_revenue,
COUNT(DISTINCT
    CASE
      WHEN LOWER(tlc.product_type) IN ('delivery') and date(rpc_redemption_datetime) between current_date('Asia/Kolkata') - 31 and '""" + str(
    formatted_yesterday) + """' THEN tlc.order_id
  END
    ) AS delivery_txn_last_30_days,
  SUM(CASE
      WHEN LOWER(tlc.product_type) IN ('delivery') and date(rpc_redemption_datetime) between current_date('Asia/Kolkata') - 31 and '""" + str(
    formatted_yesterday) + """' THEN IFNULL(tlc.revenue,0)+IFNULL(ondc_incentive,0)
  END
    ) AS delivery_revenue_last_30_days,

    COUNT(DISTINCT
    CASE
      WHEN LOWER(tlc.product_type) IN ('delivery') and date(rpc_redemption_datetime) between current_date('Asia/Kolkata') - 8 and '""" + str(
    formatted_yesterday) + """' THEN tlc.order_id
  END
    ) AS delivery_txn_last_7_days,
  SUM(CASE
      WHEN LOWER(tlc.product_type) IN ('delivery') and date(rpc_redemption_datetime) between current_date('Asia/Kolkata') - 8 and '""" + str(
    formatted_yesterday) + """' THEN IFNULL(tlc.revenue,0)+IFNULL(ondc_incentive,0)
  END
    ) AS delivery_revenue_last_7_days,
    COUNT(DISTINCT
    CASE
      WHEN LOWER(tlc.product_type) IN ('delivery') and last_day(date(rpc_redemption_datetime)) = last_day(DATE_SUB('""" + str(
    formatted_yesterday) + """', INTERVAL 1 MONTH)) THEN tlc.order_id
  END
    ) AS delivery_txn_last_month,
    sum(case when lower(tlc.product_type) in ('delivery','delivery_charge','delivery_other') and last_day(date(rpc_redemption_datetime)) = last_day(DATE_SUB('""" + str(
    formatted_yesterday) + """', INTERVAL 1 MONTH)) then ctc end)/COUNT(DISTINCT
    CASE
      WHEN LOWER(tlc.product_type) IN ('delivery') and last_day(date(rpc_redemption_datetime)) = last_day(DATE_SUB('""" + str(
    formatted_yesterday) + """', INTERVAL 1 MONTH)) THEN tlc.order_id
  END
    ) as delivery_aov_last_month
FROM
  `magicpin-14cba.txn_level_cm_reporting.txn_level_cm_reporting_main` tlc
LEFT JOIN
  `magicpin-14cba.user_as.all_txn_data` atd
ON
  tlc.txn_id = atd.txn_id
WHERE
DATE(atd.redemption_date) >= CURRENT_DATE('Asia/Kolkata') - 400
and DATE(tlc.rpc_redemption_datetime) >= CURRENT_DATE('Asia/Kolkata') - 70
  AND LOWER(atd.redemption_type) IN ('voucher',
    'magicpay',
    'recharge')
GROUP BY
  1;"""

revenue = df_from_bq(revenue)

revenue['mid'] = revenue['mid'].fillna(0).astype(int)

all_merchants = all_merchants.merge(revenue, how='left', on='mid')

ondc_data_save = """SELECT
  distinct m.id AS mid,
  (ifnull(pc.merchant_sponsored_usable_balance_percent,
      0)) AS ondc_save,
  ifnull(CAST(SPLIT(SPLIT(magicorder_day_wise_mfd,",")[safe_OFFSET(1)],":")[safe_OFFSET(1)] AS float64),
    merchant_sponsored_usable_balance_percent) + magicorder_usable_balance_percent AS delivery_save_ondc,
FROM
  `magicpin-14cba.wallet_latest.payment_commercials` pc
JOIN
  `magicpin-14cba.wallet_latest.merchant` m
ON
  m.user_id = pc.merchant_user_id
WHERE
  LOWER(ifnull(pc.commercials_type,"na")) = 'saas'
  and ifnull(pc.is_active,1)= 1
  AND m.country IN ('India')
  ;"""

ondc_data_save = df_from_bq(ondc_data_save)

ondc_data_save['mid'] = ondc_data_save['mid'].fillna(0).astype(int)

all_merchants = all_merchants.merge(ondc_data_save, how='left', on='mid')

avg_daily_orders = """SELECT distinct magic_id as mid, avg_daily_orders FROM `magicpin-analytics.locality_frame.mx_wise_avg_daily_orders`;"""

avg_daily_orders = df_from_bq(avg_daily_orders)

avg_daily_orders['mid'] = avg_daily_orders['mid'].fillna(0).astype(int)

all_merchants = all_merchants.merge(avg_daily_orders, how='left', on='mid')

# ---------------QR SCAN QUERY ADDITION------------------------


qr_scan = """with Base_data As (
SELECT distinct MID, sc.locality as locality_qr , sc.locality_id as locality_id_qr
  --CASE WHEN MIN(DATE(Scan_Date)) >= CURRENT_DATE() - 90 THEN 1 ELSE 0 END AS Installed_90D,
  --CASE WHEN sum(scan_count) > 0 and datE(scan_date) > current_date()-30 THEN 1 ELSE 0 END AS Activated_30D,
  --CASE WHEN MIN(DATE(Scan_Date)) >= CURRENT_DATE() - 1 THEN 1 ELSE 0 END AS Installed_1D,
  FROM `magicpin-14cba.merchant_growth.magic_scan_tracker_temp` sc
  left join `magicpin-14cba.wallet_latest.merchant` m on m.id=sc.mid
WHERE
DATE(Scan_Date) >= CURRENT_DATE()-90 and 
Mid is not null and sc.category_id in (1,3) and ifnull(m.merchant_priority,0)=0
group by MID, sc.merchant_name, sc.locality, sc.locality_id ,Scan_Date

union distinct
select distinct MID,locality as locality_qr , locality_id as locality_id_qr  from `magicpin-14cba.merchant_growth.magicscan_qr_map` 
where mid not in (select distinct mid from `magicpin-14cba.merchant_growth.magic_scan_tracker_temp` where mid is not null)

),
Activation_30D as (
  SELECT MID, Sum(scan_count) as scans_30D, sum(txn_count) as txns_30D
  FROM `magicpin-14cba.merchant_growth.magic_scan_tracker_temp` sc
WHERE DATE(Scan_Date) >= CURRENT_DATE - 30
group by 1
),
Activation_7D as (
  SELECT MID, Sum(scan_count) as scans_7D, sum(txn_count) as txns_7D
  FROM `magicpin-14cba.merchant_growth.magic_scan_tracker_temp` sc
WHERE DATE(Scan_Date) >= CURRENT_DATE - 7
group by 1
),
Activation_1D as (
  SELECT MID, Sum(scan_count) as scans_1D, sum(txn_count) as txns_1D
  FROM `magicpin-14cba.merchant_growth.magic_scan_tracker_temp` sc
WHERE DATE(Scan_Date) = CURRENT_DATE - 1
group by 1
),
Installation_1D as (
  Select MID, 1 as Installed_1D from `magicpin-14cba.merchant_growth.magicscan_qr_map` 
  where date(date) = current_datE()-1
),
Installation_90D as (
  Select MID, 1 as Installed_90D from `magicpin-14cba.merchant_growth.magicscan_qr_map` 
  where date(date) >= current_datE()-90
)


SELECT
  distinct bd.mid ,Installed_90D, Installed_1D, a1.scans_1D, a1.txns_1D, a7.scans_7D, a7.txns_7d, a30.scans_30D, a30.txns_30D

FROM Base_data AS bd
LEFT JOIN Activation_1D AS a1 ON bd.mid = a1.mid
LEFT JOIN Activation_7D as a7 on bd.mid = a7.mid
LEFT JOIN Activation_30D as a30 on bd.mid = a30.mid
LEFT Join Installation_1D as i1 on i1.mid=bd.mid
LEFT Join Installation_90D as i90 on i90.mid=bd.mid"""

qr_scan = df_from_bq(qr_scan)
qr_scan['mid'] = qr_scan['mid'].fillna(0).astype(int)
all_merchants = all_merchants.merge(qr_scan, how='left', on='mid')

# -------------------------------------------------------------

# --------------CLICKS VIEWS CTR------------------------


clicks_views = """SELECT 
 merchant.id as mid
, SUM(CASE WHEN date(timestamp)=current_date()-1 and events.event = 'view' and events.entity_type != 'button' then 1 else 0 end) as d1_views
, SUM(CASE WHEN date(timestamp)=current_date()-1 and events.event = 'click' then 1 else 0 end) as d1_clicks
, round(SUM(CASE WHEN date(timestamp)>=current_date()-7 and events.event = 'view' and events.entity_type != 'button' then 1 else 0 end)/7,1) as d7_avg_views
, round(SUM(CASE WHEN date(timestamp)>=current_date()-7 and events.event = 'click' then 1 else 0 end)/7,1) as d7_avg_clicks
from 
`magicpin-14cba.analytics_as.events_v2` events
inner join `magicpin-14cba.wallet_latest.merchant` merchant on CAST(merchant.user_id AS STRING) = events.subject_id
inner join `magicpin-14cba.wallet_latest.category` category on CAST(category.id AS STRING) = Cast(merchant.category_id as string)
where
events.event in ('click', 'view')
and date(events.timestamp) >=current_date()-7
group by 1"""

clicks_views = df_from_bq(clicks_views)
clicks_views['mid'] = clicks_views['mid'].fillna(0).astype(int)
all_merchants = all_merchants.merge(clicks_views, how='left', on='mid')

query = """SELECT mid,mx_type_listing FROM `magicpin-14cba.analytics_new.min_accepting_delivery` WHERE DATE(updated_at) = CURRENT_DATE('Asia/Kolkata') - 1;"""

query = df_from_bq(query)

all_merchants = all_merchants.merge(query, how='left', on='mid')

avg_take_rate = """SELECT DISTINCT m.id AS mid, IFNULL(deduction_rate, 0) AS deduction_rate FROM ( SELECT pid_mid, a.userId AS pid_mid_user_id, IFNULL(a.magicorder_repayment_redemption_percent, a.repayment_redemption_percent) AS deduction_rate FROM ( SELECT m.id AS pid_mid, ws.userId, lc.*, RANK() OVER (PARTITION BY ws.userId ORDER BY lc.repayment_target_date DESC) AS Rank FROM `magicpin-14cba.wallet_latest.loan_commercials` lc JOIN `magicpin-14cba.wallet_latest.wallet_summary` ws ON ws.id = lc.loan_wallet_id JOIN `magicpin-14cba.wallet_latest.merchant` AS m ON m.user_id = ws.UserId WHERE m.id IN ( SELECT DISTINCT IFNULL(parent_merchant_id, id) FROM `magicpin-14cba.wallet_latest.merchant`) AND loan_recovery_type = 'CTC' ) a WHERE a.rank = 1 ) ab JOIN `wallet_latest.merchant` AS m ON IFNULL(m.parent_merchant_id, m.id) = pid_mid LEFT JOIN `magicpin-14cba.analytics_new.merchant_master` AS mm ON mm.mid = m.id WHERE LOWER(mm.package_type) LIKE '%loan%'"""

avg_take_rate = df_from_bq(avg_take_rate)

all_merchants = all_merchants.merge(avg_take_rate, how='left', on='mid')

monthly_sales = """SELECT distinct mid,monthly_sales FROM `magicpin-analytics.locality_frame.quality_merchants_fixed_new`;"""

monthly_sales = df_from_bq(monthly_sales)

all_merchants = all_merchants.merge(monthly_sales, how='left', on='mid')
# -------------------------------------------------------------

all_merchants['mid'] = all_merchants['mid'].fillna(0).astype(int)
all_merchants['deduction_rate'] = all_merchants['deduction_rate'].fillna(0).astype(float)
all_merchants['mx_name'] = all_merchants['mx_name'].astype(str)
all_merchants['locality_id'] = all_merchants['locality_id'].fillna(0).astype(int)
all_merchants['locality'] = all_merchants['locality'].astype(str)
all_merchants['cluster_locality_id'] = all_merchants['cluster_locality_id'].fillna(0).astype(int)
all_merchants['highlight1'] = all_merchants['highlight1'].astype(str)
all_merchants['highlight3'] = all_merchants['highlight3'].astype(str)
all_merchants['delivery_review_count'] = all_merchants['delivery_review_count'].fillna(0).astype(int)
all_merchants['mx_type'] = all_merchants['mx_type'].astype(str)
all_merchants['cft'] = all_merchants['cft'].fillna(0).astype(int)
all_merchants['cluster_locality_name'] = all_merchants['cluster_locality_name'].astype(str)
all_merchants['mega_city'] = all_merchants['mega_city'].astype(str)
all_merchants['cash_gmv'] = all_merchants['cash_gmv'].fillna(0.0).astype(float)
all_merchants['toggle_on'] = all_merchants['toggle_on'].fillna('false').astype(str)
all_merchants['commision_value'] = all_merchants['commision_value'].fillna(0.0).astype(float)
all_merchants['delivery_commission'] = all_merchants['delivery_commission'].fillna(0.0).astype(float)
all_merchants['mfd'] = all_merchants['mfd'].fillna(0.0).astype(float)
all_merchants['mfd_c'] = all_merchants['mfd_c'].fillna(0.0).astype(float)
all_merchants['savepercent'] = all_merchants['savepercent'].fillna(0.0).astype(float)
all_merchants['is_listed'] = all_merchants['is_listed'].astype(str)
all_merchants['magicorder_mfd'] = all_merchants['magicorder_mfd'].fillna(0.0).astype(float)
all_merchants['magicpay_mfd'] = all_merchants['magicpay_mfd'].fillna(0.0).astype(float)
all_merchants['cashvoucher_mfd'] = all_merchants['cashvoucher_mfd'].fillna(0.0).astype(float)
all_merchants['combovoucher_mfd'] = all_merchants['combovoucher_mfd'].fillna(0.0).astype(float)
all_merchants['delivery_save'] = all_merchants['delivery_save'].fillna(0.0).astype(float)
all_merchants['magicpay_save'] = all_merchants['magicpay_save'].fillna(0.0).astype(float)
all_merchants['voucher_save'] = all_merchants['voucher_save'].fillna(0.0).astype(float)
all_merchants['PID'] = all_merchants['PID'].fillna(0).astype(int)
all_merchants['magicpay_status'] = all_merchants['magicpay_status'].fillna(0).astype(int)
all_merchants['magic_order_status'] = all_merchants['magic_order_status'].fillna(0).astype(int)
all_merchants['PID_CHECK'] = all_merchants['PID_CHECK'].fillna(0).astype(int)
all_merchants['voucher_status'] = all_merchants['voucher_status'].fillna(0).astype(int)
all_merchants['merchant_commercial'] = all_merchants['merchant_commercial'].fillna(0).astype(int)
all_merchants['Sub_Category'] = all_merchants['Sub_Category'].astype(str)
all_merchants['highlight5'] = all_merchants['highlight5'].astype(str)
all_merchants['online_status'] = all_merchants['online_status'].fillna(0).astype(int)
all_merchants['magicorer_toggle_on'] = all_merchants['magicorer_toggle_on'].fillna(0).astype(int)
all_merchants['online_toggle_on'] = all_merchants['online_toggle_on'].fillna(0).astype(int)
all_merchants['offline'] = all_merchants['offline'].fillna(0).astype(int)
all_merchants['offline_status'] = all_merchants['offline_status'].fillna(0).astype(int)
all_merchants['offline_state'] = all_merchants['offline_state'].fillna(0).astype(int)
all_merchants['adjusted_anyone_listed'] = all_merchants['adjusted_anyone_listed'].fillna(0).astype(int)
all_merchants['is_funded'] = all_merchants['is_funded'].fillna("No").astype(str)
all_merchants['updated_at'] = pd.to_datetime(all_merchants['updated_at'])
all_merchants['delivery_txns'] = all_merchants['delivery_txns'].fillna(0).astype(int)
all_merchants['offline_transactions'] = all_merchants['offline_transactions'].fillna(0).astype(int)
all_merchants['delivery_txns_revenue'] = all_merchants['delivery_txns_revenue'].fillna(0.0).astype(float)
all_merchants['offline_transactions_revenue'] = all_merchants['offline_transactions_revenue'].fillna(0.0).astype(float)
all_merchants['package_type'] = all_merchants['package_type'].astype(str)
all_merchants['locality_type'] = all_merchants.apply(categorize_locality, axis=1)
all_merchants['delivery_revenue_last_30_days'] = all_merchants['delivery_revenue_last_30_days'].fillna(0.0).astype(
    float)
all_merchants['delivery_revenue_last_7_days'] = all_merchants['delivery_revenue_last_7_days'].fillna(0.0).astype(float)
all_merchants['delivery_txn_last_30_days'] = all_merchants['delivery_txn_last_30_days'].fillna(0).astype(int)
all_merchants['delivery_txn_last_7_days'] = all_merchants['delivery_txn_last_7_days'].fillna(0).astype(int)
all_merchants['ondc_save'] = all_merchants['ondc_save'].fillna(0).astype(int)
all_merchants['delivery_save_ondc'] = all_merchants['delivery_save_ondc'].fillna(0).astype(int)
all_merchants['delivery_txn_last_month'] = all_merchants['delivery_txn_last_month'].fillna(0).astype(int)
all_merchants['delivery_aov_last_month'] = all_merchants['delivery_aov_last_month'].fillna(0.0).astype(float)
all_merchants['avg_daily_orders'] = all_merchants['avg_daily_orders'].fillna(0.0).astype(int)
all_merchants['ownership'] = all_merchants['ownership'].fillna("No-Owners").astype(str)
all_merchants['Installed_90D'] = all_merchants['Installed_90D'].fillna(0).astype(int)
all_merchants['Installed_1D'] = all_merchants['Installed_1D'].fillna(0).astype(int)
all_merchants['scans_1D'] = all_merchants['scans_1D'].fillna(0).astype(int)
all_merchants['txns_1D'] = all_merchants['txns_1D'].fillna(0).astype(int)
all_merchants['scans_7D'] = all_merchants['scans_7D'].fillna(0).astype(int)
all_merchants['txns_7d'] = all_merchants['txns_7d'].fillna(0).astype(int)
all_merchants['scans_30D'] = all_merchants['scans_30D'].fillna(0).astype(int)
all_merchants['txns_30D'] = all_merchants['txns_30D'].fillna(0).astype(int)

all_merchants['d1_views'] = all_merchants['d1_views'].fillna(0).astype(int)
all_merchants['d1_clicks'] = all_merchants['d1_clicks'].fillna(0).astype(int)
all_merchants['d7_avg_views'] = all_merchants['d7_avg_views'].fillna(0).astype(float)
all_merchants['d7_avg_clicks'] = all_merchants['d7_avg_clicks'].fillna(0).astype(float)
all_merchants['rating'] = all_merchants['rating'].fillna(0).astype(float)
all_merchants['mx_type_listing'] = all_merchants['mx_type_listing'].astype(str)



upload_to_bq(all_merchants)

import time
time.sleep(20)


query = """UPDATE
  `magicpin-analytics.locality_frame.quality_mx`
SET
  magicorer_toggle_on = 1
WHERE
  mid IN (
  SELECT
    DISTINCT merchant_id
  FROM
    `magicpin-analytics.catalog_mongo.import_module`
  WHERE
    LOWER(IFNULL(magic_order_updated,"na")) = 'true' )
    and datE(updated_at) = current_datE('Asia/Kolkata') ;"""

try:
  query = df_from_bq(query) 
except Exception as e:
  print(f"An error occurred: {str(e)}")


# force delisted for merchants that were newly added
# query1 = """UPDATE
#   `magicpin-analytics.locality_frame.quality_mx`
# SET
#   magicorer_toggle_on = 0
# WHERE
#   mid IN 
#   (SELECT DISTINCT mid FROM 
#   `magicpin-analytics.locality_frame.quality_mx` 
#   WHERE DATE(updated_at) = CURRENT_DATE('Asia/Kolkata')-1
#   AND mid not in (SELECT DISTINCT mid, FROM 
#   `magicpin-analytics.locality_frame.quality_mx` 
#   WHERE DATE(updated_at) = CURRENT_DATE('Asia/Kolkata') - 2))
#     and datE(updated_at) = current_datE('Asia/Kolkata')"""

# try:
#   query1 = df_from_bq(query1)
# except Exception as e:
#   print(f"An error occurred: {str(e)}")