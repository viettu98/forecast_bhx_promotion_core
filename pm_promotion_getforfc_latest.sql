select 
		pm_promotion.promotionid,
		pm_promotion.promotionname,
		pm_promotion.promotiontype,
		pm_promotion.fromdate,
		pm_promotion.todate,
		pm_promotion.mintotalmoney,
--		tmp_applystore.storeid,
		pm_promotiongiftgroup.promotiongifttype,
		pm_promotiongiftgroup.discounttype,
		pm_promotiongiftgroup.discountvalue,
		pm_promotiongift.productid AS giftproductid,
		pm_promotiongift.quantity AS giftproductquantity,
		pm_promotiongift.promotionprice,
		case 
		WHEN ((pm_promotion.fromdate >= giftprice.applydate or pm_promotion.todate >= giftprice.applydate) AND giftprice.isreview = true and giftprice.newsaleprice is not null) 
		THEN giftprice.newsaleprice 
		WHEN giftprice.newsaleprice is null and pm_promotiongift.marketprice is not null 
		THEN pm_promotiongift.marketprice
		ELSE giftprice.saleprice
		end as giftproductprice,
		COALESCE ( pm_applyproduct.productid, pm_productapplysubgroup.productid ) AS productidmain,
		COALESCE ( NULLIF (pm_promotion_applyproduct.quantity, 0), NULLIF (pm_promotion_applysubgroup.quantity, 0), NULLIF (pm_promotion.quantityperobject, 0), 0) mainproductquantity,
		price.newsaleprice,
		pm_promotion.promostoretype,
		pm_promotion.iscontinuouspromotion,
		pm_promotion.oldpromotionid,
		pm_promotion.isnosellingpower,
		pm_promotiongift.discountpercent,
		COALESCE ( pm_applyproduct.itemid, pm_productapplysubgroup.itemid ) itemid,
		COALESCE ( pm_item_exchangequantityunit.exchangequantity, 1 ) exchangequantity,
		pm_promotion.totalorderpromotiontype,
  --Loại điều kiện: 2:Theo tổng tiền đơn hàng, 4: Theo tổng số lượng
  		pm_promotion.mintotalmoneyquantity,
  --Tổng tiền mua các SP phải từ
  		CASE
	  	WHEN pm_promotion.isapplyallproduct IS NOT NULL THEN 1
	  	ELSE NULL
  		END as isapplyallproduct,
  		pm_promotion.maxtotalmoneyquantity,
  		pm_promotion.valuenextstep,
  		pm_promotion.isapplynextstep,
  		CASE WHEN pm_promotiongiftgroup.donatetype = 2 THEN pm_promotiongiftgroup.totalquantitydonate END as totalquantitydonate2,
  		pm_promotiongiftgroup.quantitypernextstep,
  		CASE WHEN pm_promotiongiftgroup.donatetype = 4 THEN pm_promotiongiftgroup.percentvalue END as totalquantitydonate4,
  		pm_promotiongiftgroup.giftproducttype,
  		pm_promotiongift.minquantity,
  		pm_promotion.percentcustomerbenefit
	FROM
		bhx_masterdata.pm_promotion
		JOIN bhx_masterdata.pm_promotiongiftgroup
			ON pm_promotion.promotionid = pm_promotiongiftgroup.promotionid
		LEFT JOIN bhx_masterdata.pm_promotion_applyproduct
			ON pm_promotion.promotionid = pm_promotion_applyproduct.promotionid
		LEFT JOIN bhx_masterdata.pm_promotion_applysubgroup
			ON pm_promotion.promotionid = pm_promotion_applysubgroup.promotionid
		LEFT JOIN bhx_masterdata.pm_promotiongift
			ON pm_promotiongiftgroup.promotiongiftgroupid = pm_promotiongift.promotiongiftgroupid
		LEFT JOIN bhx_masterdata.pm_product pm_applyproduct
			ON pm_applyproduct.productid = pm_promotion_applyproduct.productid
		LEFT JOIN bhx_masterdata.pm_product AS pm_productapplysubgroup
			ON pm_productapplysubgroup.subgroupid = pm_promotion_applysubgroup.subgroupid
			AND pm_productapplysubgroup.isdeleted = FALSE
			AND pm_productapplysubgroup.isactived = TRUE
		LEFT JOIN bhx_masterdata.pr_saleprice price
			ON price.productid IN ( COALESCE ( pm_applyproduct.productid, pm_productapplysubgroup.productid ) )
			AND price.priceareaid = 643
		LEFT JOIN bhx_masterdata.pr_saleprice giftprice
			ON giftprice.productid = pm_promotiongift.productid
			AND giftprice.priceareaid = 643
		LEFT JOIN bhx_masterdata.pm_item_exchangequantityunit
			ON pm_item_exchangequantityunit.itemid = COALESCE ( pm_applyproduct.itemid, pm_productapplysubgroup.itemid ) 
			AND pm_item_exchangequantityunit.exchangequantityunitid = COALESCE ( pm_applyproduct.quantityunitid, pm_productapplysubgroup.quantityunitid)
--		JOIN LATERAL
--		(
--			SELECT
--				pm_promotion.promotionid,
--				pm_store.storeid
--			FROM
--				bhx_masterdata.pm_store
--				LEFT JOIN bhx_masterdata.pm_promotion_applystore
--					ON pm_promotion_applystore.promotionid = pm_promotion.promotionid
--					AND pm_promotion_applystore.storeid = pm_store.storeid
--				LEFT JOIN bhx_masterdata.pm_store_storetypebyincome
--					ON pm_store_storetypebyincome.storeid = pm_store.storeid
--				LEFT JOIN bhx_masterdata.pm_promotion_applystoretypebymanual
--					ON pm_promotion_applystoretypebymanual.promotionid = pm_promotion.promotionid
--				LEFT JOIN bhx_masterdata.pm_promotion_applystoretype
--					ON pm_promotion_applystoretype.promotionid = pm_promotion.promotionid
--					AND pm_promotion_applystoretype.storetypeid = pm_store.storetypeid
--					AND pm_promotion_applystoretype.isdeleted = FALSE
--			WHERE
--				pm_store.storetypeid IN ( 1, 19, 20, 23 )
--				AND NOT EXISTS
--				(
--					SELECT 1 FROM bhx_masterdata.pm_promotion_noapplystore
--					WHERE
--						pm_promotion_noapplystore.promotionid = pm_promotion.promotionid
--						AND pm_promotion_noapplystore.storeid = pm_store.storeid
--				)
--				AND
--				(
--					(
--						pm_promotion.isapplyallstore = FALSE
--						AND
--						(
--							pm_promotion_applystore.storeid IS NOT NULL
--							OR pm_promotion_applystoretype.promotionid IS NOT NULL
--						)
--					)
--					OR
--					(
--						pm_promotion.isapplyallstore = TRUE
--						AND ( ( pm_promotion.iseliminateonlinestore = TRUE AND pm_store.storetypeid NOT IN ( 20, 23 ) ) OR pm_promotion.iseliminateonlinestore = FALSE )
--						AND ( pm_promotion_applystoretypebymanual.storetypebymanualid IS NULL OR pm_promotion_applystoretypebymanual.storetypebymanualid = pm_store_storetypebyincome.storetypebymanualid )
--					)
--				)
--		) tmp_applystore
--			ON tmp_applystore.promotionid = pm_promotion.promotionid
	 WHERE
  pm_promotion.promotionid not in ('731463','1022795','1022794')
--  AND pm_promotion.isdeleted = FALSE3
  AND isapplyallstore = true
  and pm_promotiongift.productid = '1053093000002'
--  AND COALESCE(CAST(pm_promotion.isnosellingpower AS BOOLEAN), FALSE) = FALSE
  AND pm_promotion.todate > pm_promotion.fromdate
  AND ( ( pm_promotion.isreview = TRUE AND pm_promotion.fromdate <= now( ) ) OR pm_promotion.fromdate > now( ) )
  AND (
    pm_promotion.promotiontype IN (1, 4)
    OR (
        pm_promotion.promotiontype = 8
        AND pm_promotiongiftgroup.promotiongifttype IN (3, 4, 5, 6)
      )
    )
  AND ((pm_promotion.applychannel NOT IN ('OFFLINE')) AND (pm_promotion.applychannel NOT IN ('OFFLINE,UDNV')) AND (pm_promotion.applychannel NOT IN ('UDNV')))
  AND lower(pm_promotion.promotionname) not like '%test%'
  AND pm_promotion.fromdate > '2025-06-01 00:00:00.000'
--  and pm_promotion.promotionid in (6226095)
;
--		AND
--	  (
--	   (
--	    giftprice.saleprice IS NOT NULL
--	    AND giftprice.saleprice != 0
--	   )
--	   OR pm_promotiongift.productid IS NULL
--	  )
--order by giftproductprice asc 
--limit 300000
;
select *
from bhx_masterdata.pr_saleprice giftprice
--where giftprice.productid = '9912715000822       '
--AND giftprice.priceareaid = 643
;

CASE
	  	WHEN pm_promotion.isapplyallproduct IS NOT NULL THEN 1
	  	ELSE NULL
  		END as isapplyallproduct,

)


	WHERE
  pm_promotion.promotionid != 731463
  AND COALESCE ( pm_applyproduct.subgroupid, pm_productapplysubgroup.subgroupid ) = p_subgroupid
  AND pm_promotion.isdeleted = FALSE
  AND pm_promotion.todate >= p_fromdate
  AND pm_promotion.fromdate <= p_todate
  AND pm_promotion.todate > pm_promotion.fromdate
  AND ( ( pm_promotion.isreview = TRUE AND pm_promotion.fromdate <= now( ) ) OR pm_promotion.fromdate > now( ) )
  AND pm_promotion.promotiontype IN ( 1, 4 )
  AND pm_promotion.isgrandprix = FALSE
  AND
  (
   (
    giftprice.saleprice IS NOT NULL
    AND giftprice.saleprice != 0
   )
   OR pm_promotiongift.productid IS NULL
  );