create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = getdate();
		print '===============================';
		print 'Loading Bronze Layer';
		print '===============================';

		print '-------------------------------';
		print 'Loading CRM tables';
		print '-------------------------------';

		set @start_time = getdate();
		print '>> truncating table: crm_cust_info';
		truncate table bronze.crm_cust_info;
		print '>> inserting data in table: crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\Charles\Documents\02_Projets perso\retail-etl-pipeline\sqlserver_mercedes\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2
			,fieldterminator = ','
			,tablock
		);
		set @end_time = getdate();
		print '>> Load duration : ' +cast(round(datediff(second, @start_time, @end_time),2) as nvarchar) + ' s';
		print '-----------------------';

		set @start_time = getdate();
		print '>> truncating table: crm_prd_info';
		truncate table bronze.crm_prd_info;
		print '>> inserting data in table: crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\Charles\Documents\02_Projets perso\retail-etl-pipeline\sqlserver_mercedes\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2
			,fieldterminator = ','
			,tablock
		);
		set @end_time = getdate();
		print '>> Load duration : ' +cast(round(datediff(second, @start_time, @end_time),2) as nvarchar) + ' s';
		print '-----------------------';

		set @start_time = getdate();
		print '>> truncating table: crm_sales_details';
		truncate table bronze.crm_sales_details;
		print '>> inserting data in table: crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\Charles\Documents\02_Projets perso\retail-etl-pipeline\sqlserver_mercedes\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2
			,fieldterminator = ','
			,tablock
		);
		set @end_time = getdate();
		print '>> Load duration : ' +cast(round(datediff(second, @start_time, @end_time),2) as nvarchar) + ' s';
		print '-----------------------';

		print '-------------------------------';
		print 'Loading ERP tables';
		print '-------------------------------';

		set @start_time = getdate();
		print '>> truncating table: erp_cust_az12';
		truncate table bronze.erp_cust_az12;
		print '>> inserting data in table: erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\Charles\Documents\02_Projets perso\retail-etl-pipeline\sqlserver_mercedes\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2
			,fieldterminator = ','
			,tablock
		);
		set @end_time = getdate();
		print '>> Load duration : ' +cast(round(datediff(second, @start_time, @end_time),2) as nvarchar) + ' s';
		print '-----------------------';

		set @start_time = getdate();
		print '>> truncating table: erp_loc_a101';
		truncate table bronze.erp_loc_a101;
		print '>> inserting data in table: erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\Charles\Documents\02_Projets perso\retail-etl-pipeline\sqlserver_mercedes\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow = 2
			,fieldterminator = ','
			,tablock
		);
		set @end_time = getdate();
		print '>> Load duration : ' +cast(round(datediff(second, @start_time, @end_time),2) as nvarchar) + ' s';
		print '-----------------------';

		set @start_time = getdate();
		print '>> truncating table: erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
		print '>> inserting data in table: erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\Charles\Documents\02_Projets perso\retail-etl-pipeline\sqlserver_mercedes\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow = 2
			,fieldterminator = ','
			,tablock
		);
		set @end_time = getdate();
		print '>> Load duration : ' +cast(round(datediff(second, @start_time, @end_time),2) as nvarchar) + ' s';
		print '-----------------------';

		set @batch_end_time = getdate();
		print '===============================';
		print 'Bronze Layer Loading completed in ' + cast(round(datediff(second, @batch_start_time, @batch_end_time), 2) as nvarchar) + ' s';
		print '===============================';


	end try

	begin catch
		print '===============================';
		print 'Error occured durong loading bronze layer';
		print 'Error message : ' + error_message();
		print 'Error number : ' + cast(error_number() as nvarchar);
		print '===============================';
	end catch
end
