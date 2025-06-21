USE [DataWarehouse]
GO

/****** Object:  StoredProcedure [Bronze].[load_bronze]    Script Date: 21-06-2025 18:04:57 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create   procedure [Bronze].[load_bronze] as 
Begin
	declare @start_time DATETIME, @end_time DATETIME,
	@batch_start_time datetime,@batch_end_time datetime;
	BEGIN TRY
		SET @batch_start_time=GETDATE();
		print('========================================================');
		print('Loading Bronze Layer');
		print('========================================================');

		print('--------------------------------------------------------')
		print('Loading CRM Tables');
		print('--------------------------------------------------------')

		SET @start_time=GETDATE();

		print('>>truncating table : [Bronze].[crm_cust_info]');
		truncate table [Bronze].[crm_cust_info];
		print('>>Loading table : [Bronze].[crm_cust_info]');
		bulk insert [Bronze].[crm_cust_info]
		from 
		'D:\SQL Cource STEP BY STEP\SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with
		(firstrow =2,
		fieldterminator=',',
		tablock);
		SET @end_time=GETDATE();
		PRINT('>>Load Duration: '+cast(datediff(second,@start_time,@end_time) as Nvarchar)+' Seconds');
		Print('>>--------');

		-------------------------------------------
		SET @start_time=GETDATE();
		print('>>truncating table : [Bronze].[crm_prd_info]');
		truncate table [Bronze].[crm_prd_info];
		print('>>Loading table : [Bronze].[crm_prd_info]');
		bulk insert [Bronze].[crm_prd_info]
		from 
		'D:\SQL Cource STEP BY STEP\SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with
		(firstrow =2,
		fieldterminator=',',
		tablock);
		PRINT('>>Load Duration: '+cast(datediff(second,@start_time,@end_time) as Nvarchar)+' Seconds');
		Print('>>--------');
		-----------------------------------------------
		SET @start_time=GETDATE();
		print('>>truncating table : [Bronze].[crm_sales_details]');
		truncate table [Bronze].[crm_sales_details];
		print('>>Loading table : [Bronze].[crm_sales_details]');
		bulk insert [Bronze].[crm_sales_details]
		from 
		'D:\SQL Cource STEP BY STEP\SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with
		(firstrow =2,
		fieldterminator=',',
		tablock);
		PRINT('>>Load Duration: '+cast(datediff(second,@start_time,@end_time) as Nvarchar)+' Seconds');
		Print('>>--------');

		print('--------------------------------------------------------')
		print('Loading ERP Tables');
		print('--------------------------------------------------------')

		--------------------------------------------------------------------
		SET @start_time=GETDATE();
		print('>>truncating table : [Bronze].[erp_cust_az12]');
		truncate table [Bronze].[erp_cust_az12];
		print('>>Loading table : [Bronze].[erp_cust_az12]');
		bulk insert [Bronze].[erp_cust_az12]
		from 
		'D:\SQL Cource STEP BY STEP\SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with
		(firstrow =2,
		fieldterminator=',',
		tablock);
		PRINT('>>Load Duration: '+cast(datediff(second,@start_time,@end_time) as Nvarchar)+' Seconds');
		Print('>>--------');
		--------------------------------------------
		SET @start_time=GETDATE();
		print('>>truncating table : [Bronze].[erp_loc_a101]');
		truncate table [Bronze].[erp_loc_a101];
		print('>>Loading table : [Bronze].[erp_loc_a101]');
		bulk insert [Bronze].[erp_loc_a101]
		from 
		'D:\SQL Cource STEP BY STEP\SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with
		(firstrow =2,
		fieldterminator=',',
		tablock);
		PRINT('>>Load Duration: '+cast(datediff(second,@start_time,@end_time) as Nvarchar)+' Seconds');
		Print('>>--------');
		--------------------------------------------------
		SET @start_time=GETDATE();
		print('>>truncating table : [Bronze].[erp_px_cat_g1v2]');
		truncate table [Bronze].[erp_px_cat_g1v2];
		print('>>Loading table : [Bronze].[erp_px_cat_g1v2]');

		bulk insert [Bronze].[erp_px_cat_g1v2]
		from 
		'D:\SQL Cource STEP BY STEP\SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with
		(firstrow =2,
		fieldterminator=',',
		tablock);
		PRINT('>>Load Duration: '+cast(datediff(second,@start_time,@end_time) as Nvarchar)+' Seconds');
		Print('>>--------');
		SET @batch_end_time=GETDATE();
		print('========================================================');
		print('Loading Bronze Layer is Completed');
		print('>>Load Duration: '+cast(datediff(second,@batch_start_time,@batch_end_time) as Nvarchar)+' Seconds');
		print('========================================================');
	END TRY
	BEGIN CATCH
		print('========================================================');
		print('ERROR OCCURED DURING LOADING BRRONZE LAYER');
		print('ERROR MESSAGE'+error_message());
		print('ERROR MESSAGE'+cast(error_number() as nvarchar));
		print('ERROR MESSAGE'+cast(error_state() as nvarchar));
		print('========================================================');
	END CATCH
END
GO


