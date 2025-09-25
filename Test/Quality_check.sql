print '============================'
print 'DATA QUALITY CHECK'
print '============================'

print '----------------------------'
print 'checking unwanted spaces'
print '----------------------------'

select
cst_key,
cst_id,
cst_firstname,
cst_lastname,
cst_material_status,
cst_gndr,
cst_create_date
from silver.crm_cust_info
where cst_key !=        trim(cst_key)
or cst_firstname!=      trim(cst_firstname)
or cst_lastname!=       trim(cst_lastname)
or cst_gndr!=           trim(cst_gndr)
or cst_material_status!=trim(cst_material_status);


print '----------------------------'
print 'check for duplicates and null'
print '----------------------------'

select
prd_id,cat_id,prd_key,
count(*)
from silver.crm_prd_info
group by prd_id,cat_id,prd_key
having count(*)>1 or prd_id is null;


print '----------------------------'
print 'DATA STANDARDISATION AND CONSISTENCY'
print '----------------------------'

SELECT DISTINCT prd_key
from silver.crm_prd_info

SELECT * FROM silver.crm_prd_info
where prd_start_dt>prd_end_dt


