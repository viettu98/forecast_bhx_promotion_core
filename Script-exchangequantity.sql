
select i.itemid , i.productid , i.productname , e.exchangequantity 
from bhx_masterdata.pm_product i  
join bhx_masterdata.pm_item_exchangequantityunit e 
on (i.itemid = e.itemid) and (i.quantityunitid = e.exchangequantityunitid)
where i.itemid = '1608001340    ';


SELECT 
    pp.productid,
	pp.productname, 
	pp.itemid,
	e.exchangequantity
FROM
    masterdata.pm_product pp
left Join
    bhx_masterdata.pm_item_exchangequantityunit e
on
	(pp.itemid = e.itemid) and (pp.quantityunitid = e.exchangequantityunitid)
WHERE 
    pp.itemid IN('2006000630','2204000071','2006000354','1907000832','1607001868','2107004044','2008000058','1607002645','1712003303','1607002604','2209002124','1607002985','1607000387','1607002606','2405000776','2106000757','1607000369','1703000851','1810000374','2405000775','1709000198','2307002888','1607002627','1904000235','2011000249','1607002608','2108002922','1607003008','1607002605','1607003181','2312000217','2310000876','2212000702','1703001311','2009001058','2307002298','2111004755','1607000373','2109001813','2406000111','2106001302','1607001835','1607002620','2310000275','2210000027','2106000758','2112004785','1607002345','2302001072','1607001811','2008000265','2309015726','2305056605','2307000300','2106001303','1607002219','1607002213','1607002215','2310000867','2107001733','1811000454','1711001626','2005001048','2008000059','1811000025','1607002315','1809000345','1912000057','1909000697','1908000128','1703000839','2105000533','1907001261','1803000967','2309016926','2207000327','1907000304','1911000083','2104000181','2406000701','2303016987','1802001088','2404000177','1910000149','2405000711','1608001012','1711000606','1607000393','2304047971','2107001834','1607000320','1902000157','2005000088','2005000090','2404000173','2306000857','1607000652','1910000167','1911000094','2404000143','1607002292','2003000278')
    ;
    
    
    
    SELECT
DISTINCT(pm_product.productid), pm_product.productidref,
pm_product.maingroupid, pm_product.subgroupid, pm_product.modelid, pm_product.itemid, pm_product.productcolorid,
-- 1 sp bất kỳ - đơn vị của sp đó - sp cơ sở - đơn vị cơ sở - sl quy đổi
pm_product.productname, --Tên sp
pm_quantityunit.quantityunit, --ĐVT
pm_baseproduct.productid baseproductid, --SP cơ sở
pm_baseproduct.productname baseproductname, --Tên Sp cơ sở
pm_basequantityunit.quantityunit basequantityunit, --ĐVT cơ sở
COALESCE(pm_item_exchangequantityunit.exchangequantity, 1) exchangequantity, --SL quy đổi
pm_product.productlength, pm_product.productwidth, pm_product.productheight, pm_product.productsize, pm_product.productweight
FROM MASTERDATA.pm_product
left JOIN MASTERDATA.pm_brand
 ON pm_product.brandid = MASTERDATA.pm_brand.brandid
JOIN MASTERDATA.pm_item
 ON pm_item.itemid = pm_product.itemid
JOIN MASTERDATA.pm_product pm_baseproduct
 ON pm_baseproduct.productid = pm_item.productid
LEFT JOIN MASTERDATA.pm_item_exchangequantityunit
 ON pm_item_exchangequantityunit.itemid = pm_product.itemid AND pm_item_exchangequantityunit.exchangequantityunitid = pm_product.quantityunitid
JOIN MASTERDATA.pm_quantityunit
 ON pm_quantityunit.quantityunitid = pm_product.quantityunitid
JOIN MASTERDATA.pm_quantityunit pm_basequantityunit
 ON pm_basequantityunit.quantityunitid = pm_baseproduct.quantityunitid
JOIN MASTERDATA.PM_PRODUCT_PRODUCTSTATUS
 ON pm_product.productid = PM_PRODUCT_PRODUCTSTATUS.productid
WHERE PM_PRODUCT_PRODUCTSTATUS.COMPANYID = 2
and pm_product.itemid IN('2006000630','2204000071','2006000354','1907000832','1607001868','2107004044','2008000058','1607002645','1712003303','1607002604','2209002124','1607002985','1607000387','1607002606','2405000776','2106000757','1607000369','1703000851','1810000374','2405000775','1709000198','2307002888','1607002627','1904000235','2011000249','1607002608','2108002922','1607003008','1607002605','1607003181','2312000217','2310000876','2212000702','1703001311','2009001058','2307002298','2111004755','1607000373','2109001813','2406000111','2106001302','1607001835','1607002620','2310000275','2210000027','2106000758','2112004785','1607002345','2302001072','1607001811','2008000265','2309015726','2305056605','2307000300','2106001303','1607002219','1607002213','1607002215','2310000867','2107001733','1811000454','1711001626','2005001048','2008000059','1811000025','1607002315','1809000345','1912000057','1909000697','1908000128','1703000839','2105000533','1907001261','1803000967','2309016926','2207000327','1907000304','1911000083','2104000181','2406000701','2303016987','1802001088','2404000177','1910000149','2405000711','1608001012','1711000606','1607000393','2304047971','2107001834','1607000320','1902000157','2005000088','2005000090','2404000173','2306000857','1607000652','1910000167','1911000094','2404000143','1607002292','2003000278')
    ;