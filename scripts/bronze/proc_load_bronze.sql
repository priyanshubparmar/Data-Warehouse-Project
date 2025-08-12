/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = GETDATE();
		print'==========================================================';
		print'Loading Bronze Layer';
		print'==========================================================';

		print'----------------------------------------------------------';
		print'Loading CRM Tables';
		print'----------------------------------------------------------';

		set @start_time = getdate();
		print'>> truncate table bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;

		print '>> Inserting Data INfo: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\parma\OneDrive\Desktop\data Warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print'>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print'---------------------------------------------------------------------------------------------'

		set @start_time = getdate();
		print'>> truncate table bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
		print '>> Inserting Data INfo: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\parma\OneDrive\Desktop\data Warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = getdate();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
		print'---------------------------------------------------------------------------------------------'

		set @start_time = getdate();
		print'>> truncate table bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
		print '>> Inserting Data INfo: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\parma\OneDrive\Desktop\data Warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = getdate();
		print'>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print'---------------------------------------------------------------------------------------------'

		set @start_time = getdate();
		print'----------------------------------------------------------';
		print'Loading ERP Tables';
		print'----------------------------------------------------------';

		print'>> truncate table bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;
		print '>> Inserting Data INfo: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\parma\OneDrive\Desktop\data Warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = getdate();
		print'>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print'---------------------------------------------------------------------------------------------'

		set @start_time = getdate();
		print'>> truncate table bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;
		print '>> Inserting Data INfo: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\parma\OneDrive\Desktop\data Warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = getdate();
		print'>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print'---------------------------------------------------------------------------------------------'

		set @start_time = getdate();
		print'>> truncate table bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
		print '>> Inserting Data INfo: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\parma\OneDrive\Desktop\data Warehouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = getdate();
		print'>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print'---------------------------------------------------------------------------------------------'


		set @batch_end_time = getdate();
		print'==================================================================';
		print'Loading Bronze Layer Is completed';
		print'    - Total Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time)as Nvarchar)+ 'seconds';
		end try
		begin catch
			print'========================================='
			print'Error occured during loading bronze layer'
			print'Error Message' + error_message();
			print 'Error Message' + cast (error_number() as nvarchar);
			print 'Error Message' + cast (error_state() as nvarchar);
			print'========================================='
		end catch
end;
