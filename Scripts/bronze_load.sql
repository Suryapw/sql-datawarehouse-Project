
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME;
	BEGIN TRY
		SET @start_time=GETDATE();
		PRINT('===========================================');
		PRINT('BRONZE LAYER LOADED');
		PRINT('===========================================');

		PRINT('~~~~~~~~~~~~~~~~~~~~~~~~');
		PRINT 'CRM DETAILS';
		PRINT('~~~~~~~~~~~~~~~~~~~~~~~~');

		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\surya\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		print '>> load duration:' +CAST(datediff(second,@start_time,@end_time) AS NVARCHAR) +'seconds';

		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\surya\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		print '>> load duration:' +CAST(datediff(second,@start_time,@end_time) AS NVARCHAR) +'seconds';

        SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\surya\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		print '>> load duration:' +CAST(datediff(second,@start_time,@end_time) AS NVARCHAR) +'seconds';



		PRINT('~~~~~~~~~~~~~~~~~~~~~~~~');
		PRINT 'ERP DETAILS';
		PRINT('~~~~~~~~~~~~~~~~~~~~~~~~');

		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\surya\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();


		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\surya\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		print '>> load duration:' +CAST(datediff(second,@start_time,@end_time) AS NVARCHAR) +'seconds';



		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\surya\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		print '>> load duration:' +CAST(datediff(second,@start_time,@end_time) AS NVARCHAR) +'seconds';
		SET @end_time=GETDATE()
		print 'duration of bronze layer:'+CAST(datediff(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
	END TRY
	BEGIN CATCH
	PRINT 'ERROR OCCURED'
	PRINT ' ERROR:'+ ERROR_MESSAGE();
	END CATCH
END
