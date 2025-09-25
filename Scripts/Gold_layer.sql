SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_material_status,
	ci.cst_create_date,
	ca.bdate,
	la.cntry

	from silver.crm_cust_info ci
	left join silver.erp_cust_az12 ca
	on ci.cst_key=ca.cid
	left join silver.erp_loc_a101 la
	on  ci.cst_key=la.cid
	

