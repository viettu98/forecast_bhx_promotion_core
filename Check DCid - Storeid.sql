select *
from purchasing.pom_orderarea_logisticstore
where pom_orderarea_logisticstore.logisticstoreid = 1522;

select *
from bachhoaxanh.show_store_product;

select *
from purchasing.pom_orderarea_calcstore
where orderareaid = 1522;

select *
from purchasing.pom_orderarea po
where orderareaname like '%FRESH%';
where po.orderareatypeid in (40);


select distinct
    tmp.logisticstoreid AS dcid,
     tmp.calcstoreid AS storeid,
    tmp.itemid
FROM
    (
        SELECT
            pom_orderarea_logisticstore.logisticstoreid,
            pom_orderarea_calcstore.calcstoreid,
            pm_product.itemid
        FROM
            purchasing.pom_orderarea
        JOIN
            purchasing.pom_orderarea_logisticstore
        ON
            pom_orderarea_logisticstore.orderareaid = pom_orderarea.orderareaid
        JOIN
            purchasing.pom_orderarea_calcstore
        ON
            pom_orderarea_calcstore.orderareaid = pom_orderarea.orderareaid
        JOIN
            purchasing.pom_productstockdays
        ON
            pom_productstockdays.orderareaid = pom_orderarea.orderareaid
        JOIN
            bhx_masterdata.pm_product
        ON
            pm_product.productid = pom_productstockdays.productid
        WHERE
            pom_orderarea_logisticstore.logisticstoreid = 1522
--        pom_orderarea_calcstore.calcstoreid = 3750
        and pm_product.itemid = '2405000711'
        AND
            pom_orderarea.isdeleted = FALSE
        AND
            pom_orderarea_logisticstore.isdefault = TRUE
        AND
            EXISTS
            (
                SELECT 1
                FROM
                    bachhoaxanh.show_store_product
                JOIN
                    bhx_masterdata.pm_product
                ON
                    pm_product.productid = show_store_product.productid
                WHERE
                    show_store_product.storeid = 1522
                AND
                    show_store_product.statusid = 3
                AND
                    pm_product.itemid = pm_product.itemid
                GROUP BY
                    pm_product.itemid
            )
        GROUP BY
            pom_orderarea_logisticstore.logisticstoreid,
            pom_orderarea_calcstore.calcstoreid,
            pm_product.itemid
    ) tmp
WHERE
    EXISTS
    (
        SELECT 1
        FROM
            bachhoaxanh.show_store_product
        JOIN
            bhx_masterdata.pm_product
        ON
            pm_product.productid = show_store_product.productid
        WHERE
            show_store_product.storeid = tmp.calcstoreid
        AND
            show_store_product.statusid = 3
        AND
            pm_product.itemid = tmp.itemid
        GROUP BY
            pm_product.itemid
    );