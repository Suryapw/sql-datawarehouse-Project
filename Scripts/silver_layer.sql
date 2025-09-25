CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time datetime,@end_time datetime;
        begin try
        set @start_time=GETDATE();

        PRINT('===========================================');
	    PRINT('SILVER LAYER LOADED');
	    PRINT('===========================================');

	    PRINT('~~~~~~~~~~~~~~~~~~~~~~~~');
	    PRINT 'CRM DETAILS';
	    PRINT('~~~~~~~~~~~~~~~~~~~~~~~~');

        print '>> truncating table customer info:';
        TRUNCATE TABLE silver.crm_cust_info;
        print '>>data inserted';

        SET @start_time=GETDATE()
        INSERT INTO silver.crm_cust_info(
        cst_key,
        cst_id,
        cst_firstname,
        cst_lastname,
        cst_material_status,
        cst_gndr,
        cst_create_date
        )

        select
        cst_key,
        cst_id,
        trim(cst_firstname) as cst_firstname,
        trim(cst_lastname) as cst_lastname,
        CASE WHEN upper(trim(cst_material_status))= 'S' THEN 'Single'
	         WHEN upper(trim(cst_material_status))= 'M' THEN 'Married'
	         else 'N/A'
        END cst_material_status,

        CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
             WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
             else 'N/A'
        END cst_gndr,
        cst_create_date

	        from(
	    	    select *,ROW_NUMBER() over (partition by cst_id order by cst_create_date DESC) as flag_last
	    	    from bronze.crm_cust_info
	    	    where cst_id is not null
	    	    )t
        where flag_last=1;
        SET @end_time=GETDATE()
        print'time taken to load table'+cast(datediff(second,@start_time,@end_time) as nvarchar) +'seconds'
        

        SET @start_time=GETDATE()
        print '>> truncating table product info:';
        TRUNCATE TABLE silver.crm_prd_info;
        print '>>insert the data';
        INSERT INTO silver.crm_prd_info(
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
        )
        SELECT
        prd_id,
        REPLACE(substring(prd_key,1,5),'-','_') AS cat_id,
        SUBSTRING(prd_key,7,len(prd_key)) AS prd_key,
        prd_nm,
        isnull(prd_cost,0) as prd_cost,
        CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
             WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
             WHEN UPPER(TRIM(prd_line))='S' THEN 'other sales'
             WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
             else 'n/a'
        END AS prd_line,   
        CAST(prd_start_dt as date) as prd_start_dt,
        cast(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS date) as prd_end_dt
        FROM bronze.crm_prd_info
        order by prd_id;
        set @end_time=GETDATE();
            print'time taken to load table'+cast(datediff(second,@start_time,@end_time) as nvarchar) +'seconds';


        SET @start_time=GETDATE()
        print '>> truncating table dales details:';
        TRUNCATE TABLE silver.crm_sales_details;
        print '>>insert the data';
        insert into silver.crm_sales_details(
        sls_ord_num,
        sls_cust_id,
        sls_prd_key,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_price,
        sls_quantity
        )
        select
        trim(sls_ord_num) as sls_ord_num,
        trim(sls_prd_key) as sls_cust_id,
        sls_prd_key,
        case when sls_order_dt<=0 or len(sls_order_dt)!=8 then NULL
             else cast(cast(sls_order_dt as varchar)as date)
             end as sls_order_dt,

        case when sls_ship_dt<=0 or len(sls_ship_dt)!=8 then NULL
             else cast(cast(sls_ship_dt as varchar)as date)
             end as sls_ship_dt,

        case when sls_due_dt<=0 or len(sls_due_dt)!=8 then NULL
             else cast(cast(sls_due_dt as varchar)as date)
             end as sls_due_dt,

        case when sls_sales<=0 or sls_sales is null then sls_quantity*ABS(sls_price)
            else sls_sales
            end as sls_sales,

        case when sls_price=0 or sls_price is NULL then ABS(sls_sales)/NULLIF(sls_quantity,0)
             when sls_price<0 THEN ABS(sls_sales)/sls_quantity
             else sls_price
             end as sls_price,

        sls_quantity
        from bronze.crm_sales_details;
        set @end_time=GETDATE()
        print'time taken to load table'+cast(datediff(second,@start_time,@end_time) as nvarchar) +'seconds'

        PRINT('~~~~~~~~~~~~~~~~~~~~~~~~');
		PRINT 'ERP DETAILS';
		PRINT('~~~~~~~~~~~~~~~~~~~~~~~~');

        SET @start_time=GETDATE()
        print '>> truncating table location_customers:';
        TRUNCATE TABLE silver.erp_loc_a101;
        print '>>insert the data';
        insert into silver.erp_loc_a101(
        cid,
        cntry)
        select
        replace(cid,'-','') cid,
        case when trim(cntry)='DE' then 'Germany'
	        when trim(cntry) in ('US','USA') THEN 'United States'
	        when cntry is NULL or trim(cntry)='' THEN 'n/a'
	        else trim(cntry) 
        end as cntry
       
        from bronze.erp_loc_a101;
        set @end_time=GETDATE()
        print'time taken to load table'+cast(datediff(second,@start_time,@end_time) as nvarchar) +'seconds'


        SET @start_time=GETDATE()
        print '>> truncating table product catogories:';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        print '>>insert the data';
        insert into silver.erp_px_cat_g1v2(
        id,
        cat,
        subcat,
        maintenance)

        select
        id,
        cat,
        subcat,
        maintenance
        from bronze.erp_px_cat_g1v2;
        set @end_time=GETDATE();
        print'time taken to load table'+cast(datediff(second,@start_time,@end_time) as nvarchar) +'seconds';
   set @end_time=GETDATE();
   print'-------------------------------------------------------------------------------------------'
   print'time taken to load complete silver layer'+cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds';
   print'-------------------------------------------------------------------------------------------'




   print'==========-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-'
   	insert into silver.erp_cust_az12(
	cid,
	bdate,
	gen)
	select 
	case when cid like 'NAS%' THEN substring(cid,4,len(cid))
		else cid
	end as cid,
	case when bdate >getdate() then null
		else bdate
	end as bdate,

	case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
		 when upper(trim(gen)) in ('M','MALE') then 'Male'
		 ELSE 'n/a'
	end as gen
	from bronze.erp_cust_az12;


   end try
   BEGIN CATCH
	PRINT 'ERROR OCCURED'
	PRINT ' ERROR:'+ ERROR_MESSAGE();
	END CATCH
 END
