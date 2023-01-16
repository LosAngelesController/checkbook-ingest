const execSync = require('child_process').execSync;

var randomstring = require("randomstring");
// import { execSync } from 'child_process';  // replace ^ if using ES modules

const config = require('./config.json');

function generateIdempotency() {
    return `${Date.now()}${randomstring.generate({
      length: 10,
      charset: 'alphanumeric',
      capitalization: 'lowercase'
    })}`;
  }

const nameofidemp = generateIdempotency();

const csvname = 'inputs/' + nameofidemp + '.csv';

const makenewfolder = execSync('[ -d inputs ] || mkdir inputs', { encoding: 'utf-8' });  // the default is 'buffer'
console.log('Output was:\n', makenewfolder);



console.log('making alias table');


var listofalias = [
  'DROP TABLE IF EXISTS aliastable;', // drop the table if it exists
  // ensure the table at least exists, if not, create it
'CREATE TABLE IF NOT EXISTS aliastable (input varchar(255) PRIMARY KEY,showas varchar(255));',
//insert the alias into the table
`INSERT INTO aliastable (input, showas) VALUES ('MICROSOFT CORP', 'MICROSOFT CORPORATION') ON CONFLICT DO NOTHING;`,
]

const setup = `PGPASSWORD=${config.password} psql -U ${config.username} -h ${config.hostname}`

const aliaspostgrs = execSync(
`${setup} -d postgres -c "${listofalias.join('')}"`, 
{ encoding: 'utf-8',
stdio: 'inherit'});  // the default is 'buffer'

console.log('start uploading alias table');

const ingestaliasfile = execSync(
  `${setup} -d postgres -c "\\copy aliastable from './aliases.csv' DELIMITER ',' CSV HEADER;"`, 
   { encoding: 'utf-8',
   stdio: 'inherit'});  // the default is 'buffer'

console.log('done making alias table');
if (true){
const output = execSync(
    'wget https://controllerdata.lacity.org/api/views/pggv-e4fn/rows.csv?accessType=DOWNLOAD -O '
     + csvname, 
     { encoding: 'utf-8',
     stdio: 'inherit'});  // the default is 'buffer'
}


     const postgresingest = execSync(
        `${setup} -d postgres -c "
        
        CREATE TABLE IF NOT EXISTS init${nameofidemp} (
          id_number integer PRIMARY KEY,
    fiscal_year smallint,
    department_name VARCHAR(255),
    vendor_name VARCHAR(255),
    transaction_date DATE,
    dollar_amount decimal,
    authority VARCHAR(255),
    business_tax_registration_certificate VARCHAR(255),
    government_activity VARCHAR(255),
    fund_group_name VARCHAR(255),
    fund_type VARCHAR(255),
    fund_name VARCHAR(255),
    fund VARCHAR(255),
    account_name VARCHAR(255),
    account_code VARCHAR(255),
    transaction_id VARCHAR(255),
    expenditure_type VARCHAR(255),
    settlement_judgment VARCHAR(255),
    fiscal_month_number smallint,
    fiscal_year_month VARCHAR(255),
    fiscal_year_quarter VARCHAR(255),
    calendar_month_number smallint,
    calendar_month_year VARCHAR(255),
    calendar_month VARCHAR(255),
    data_source VARCHAR(255),
    authority_name VARCHAR(255),
    authority_link VARCHAR(255),
    department_number VARCHAR(255),
    program VARCHAR(255),
    vendor_id VARCHAR(255),
    zip VARCHAR(255),
    payment_method VARCHAR(255),
    payment_status VARCHAR(255),
    inv_num VARCHAR(255),
    invoice_due_date DATE,
    invoice_discount_due_date DATE,
    inv_date DATE,
    inv_line decimal,
    invoice_distribution_line decimal,
    po_num VARCHAR(255),
    description VARCHAR(255),
    detailed_item_description VARCHAR(255),
    unit_price decimal,
    unit_of_measure VARCHAR(255),
    quantity decimal,
    sales_tax_percent decimal,
    sales_tax decimal,
    discount decimal,
    receiver_id VARCHAR(4095),
    po_date DATE,
    po_line_number decimal,
    procurement_organization VARCHAR(255),
    buyer_name VARCHAR(255),
    supplier_city VARCHAR(255),
    supplier_country VARCHAR(255),
    bu_name VARCHAR(255),
    site_location VARCHAR(255),
    item_code VARCHAR(255),
    item_code_name VARCHAR(255),
    currency VARCHAR(255),
    value_of_spend decimal,
    vendor_num VARCHAR(255)
          )
        "`, 
         { encoding: 'utf-8',
         stdio: 'inherit'});  // the default is 'buffer'

         console.log(`table made init${nameofidemp}`);
         console.log('ingesting into postgres', new Date);

         const csvstep2 = execSync(
          `${setup} -d postgres -c "\\copy init${nameofidemp} from '${csvname}' DELIMITER ',' CSV HEADER;"`, 
           { encoding: 'utf-8',
           stdio: 'inherit'});  // the default is 'buffer'
  

           console.log(`injest done at ${new Date}`) 


          const listofsqlrequests = [
            //start the series of transactions, don't save it until the end.
            'BEGIN;',
            //perform an alias lookup
            `CREATE TABLE IF NOT EXISTS aliased${nameofidemp} AS (SELECT * from init${nameofidemp} LEFT JOIN aliastable ON init${nameofidemp}.vendor_name = aliastable.input);`,
            `DROP TABLE IF EXISTS losangelescheckbooknew;`,
            //create the real table
            `CREATE TABLE IF NOT EXISTS losangelescheckbooknew AS (SELECT *, COALESCE ( showas,vendor_name ) as vendor_name_new FROM aliased${nameofidemp});`,

            //delete column vendor_name
            `ALTER TABLE losangelescheckbooknew DROP COLUMN vendor_name;`,
            `ALTER TABLE losangelescheckbooknew DROP COLUMN showas;`,
            `ALTER TABLE losangelescheckbooknew DROP COLUMN input;`,

            //rename the column vendor_name_new to vendor_name
            `ALTER TABLE losangelescheckbooknew RENAME COLUMN vendor_name_new TO vendor_name;`,
            //rename the aliased table into losangelescheckbook
              //drop the init table
              `DROP TABLE IF EXISTS init${nameofidemp};`,
              `DROP TABLE IF EXISTS aliased${nameofidemp};`,
              //drop the old table
              `DROP TABLE IF EXISTS losangelescheckbook;`,

              `DROP TABLE IF EXISTS vendors_summed;`,

            //rename the new table to the old table
            `ALTER TABLE losangelescheckbooknew RENAME TO losangelescheckbook;`,
            //create the vendor lookup table
            `CREATE TABLE IF NOT EXISTS vendor_summed AS (SELECT count(*), sum(dollar_amount), vendor_name FROM losangelescheckbook GROUP BY vendor_name ORDER BY SUM(dollar_amount) desc);`,
            
            `COMMIT;`
          ]
          const sqlimport = execSync(
            `${setup} -d postgres -c "${listofsqlrequests.join('')}"`, 
             { encoding: 'utf-8',
             stdio: 'inherit'});  // the default is 'buffer'