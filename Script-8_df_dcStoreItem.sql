

SELECT
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


SELECT
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
        AND
            pom_orderarea.isdeleted = FALSE
        AND
            pom_orderarea_logisticstore.isdefault = TRUE
        AND EXISTS
        (
            SELECT 1
            FROM
                bachhoaxanh.show_store_product
            JOIN
                bhx_masterdata.pm_product AS sp_pm_product
            ON
                sp_pm_product.productid = show_store_product.productid
            WHERE
                show_store_product.storeid = 116
            AND
                show_store_product.statusid = 3
            AND
                sp_pm_product.itemid = pm_product.itemid
            GROUP BY
                sp_pm_product.itemid
        )
        GROUP BY
            pom_orderarea_logisticstore.logisticstoreid,
            pom_orderarea_calcstore.calcstoreid,
            pm_product.itemid
    ) tmp
WHERE EXISTS
    (
        SELECT 1
        FROM
            bachhoaxanh.show_store_product
        JOIN
            bhx_masterdata.pm_product AS sp_pm_product
        ON
            sp_pm_product.productid = show_store_product.productid
        WHERE
            show_store_product.storeid = tmp.calcstoreid
        AND
            show_store_product.statusid = 3
        AND
            sp_pm_product.itemid = tmp.itemid
        GROUP BY
            sp_pm_product.itemid
    );


select *
from purchasing.pom_orderarea_logisticstore;

select *
from bachhoaxanh.show_store_product;




SELECT DISTINCT
    pom_orderarea_calcstore.calcstoreid
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
AND
    pom_orderarea.isdeleted = false 
AND
    pom_orderarea_logisticstore.isdefault = true 
AND EXISTS
(
    SELECT 1
    FROM
        bachhoaxanh.show_store_product show_dc_product
    JOIN
        bhx_masterdata.pm_product pm_dcproduct
    ON
        pm_dcproduct.productid = show_dc_product.productid
    WHERE
        show_dc_product.storeid = 1522
    AND
        show_dc_product.statusid = 3
    AND
        pm_dcproduct.itemid = pm_product.itemid
)


 
