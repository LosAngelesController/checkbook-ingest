const execSync = require('child_process').execSync;

var randomstring = require("randomstring");

const fs = require('fs');
// import { execSync } from 'child_process';  // replace ^ if using ES modules

const config = require('./config.json');

function generateIdempotency() {
    return `${Date.now()}${randomstring.generate({
      length: 10,
      charset: 'alphanumeric',
      capitalization: 'lowercase'
    })}`;
  }
  
  function shortIdempotency() {
    return `${randomstring.generate({
      length: 7,
      charset: 'alphabetic',
      capitalization: 'lowercase'
    })}`;
  }
 export function main () {
    
const nameofidemp = generateIdempotency();

const csvname = 'inputs/' + nameofidemp + '.csv';
execSync('if [ -d inputs ]; then rm -rf inputs; fi', { encoding: 'utf-8' });
const makenewfolder = execSync('[ -d inputs ] || mkdir inputs', { encoding: 'utf-8' });  // the default is 'buffer'
console.log('Output was:\n', makenewfolder);


console.log('making alias table');



var listofalias = [
  'BEGIN;',
  'DROP TABLE IF EXISTS aliastable;', // drop the table if it exists
  // ensure the table at least exists, if not, create it
'CREATE TABLE IF NOT EXISTS aliastable (input varchar(255) PRIMARY KEY,showas varchar(255));',
//insert the alias into the table
`INSERT INTO aliastable (input, showas) VALUES ('MICROSOFT CORP', 'MICROSOFT CORPORATION') ON CONFLICT DO NOTHING;`,
"CREATE EXTENSION IF NOT EXISTS pg_trgm;CREATE EXTENSION IF NOT EXISTS btree_gin;",
'COMMIT;'
]

const setup = ` PGPASSWORD=${config.password} psql "sslmode=verify-ca sslrootcert=server-ca.pem sslcert=client-cert.pem sslkey=client-key.pem hostaddr=${config.hostname} port=5432 user=${config.username} dbname=postgres"`

const aliaspostgrs = execSync(
`${setup} -c "${listofalias.join('')}"`, 
{ encoding: 'utf-8',
stdio: 'inherit'});  // the default is 'buffer'

console.log('start uploading alias table');

const ingestaliasfile = execSync(
  `${setup} -c "\\copy aliastable from './aliases.csv' DELIMITER ',' CSV HEADER;"`, 
   { encoding: 'utf-8',
   stdio: 'inherit'});  // the default is 'buffer'
     
     const timeofdownload = new Date().toISOString().replace('T'," ").replace("Z", "")

console.log('done making alias table');
if (true){
const output = execSync(
    'wget https://controllerdata.lacity.org/api/views/pggv-e4fn/rows.csv?accessType=DOWNLOAD -O '
     + csvname, 
     { encoding: 'utf-8',
     stdio: 'inherit'});  // the default is 'buffer'
}

     const sizeoffile = fs.statSync(csvname).size;

     const postgresingest = execSync(
        `${setup}  -c "
        
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
          `${setup} --echo-all -c "\\copy init${nameofidemp} from '${csvname}' DELIMITER ',' CSV HEADER;"`, 
           { encoding: 'utf-8',
           stdio: 'inherit'});  // the default is 'buffer'
  

           console.log(`injest done at ${new Date}`) 

          function executesqlarray(sqlarray:Array<string>) {
            execSync(
              `${setup} --echo-all -c "${sqlarray.join('')}"`, 
               { encoding: 'utf-8',
               stdio: 'inherit'});  // the default is 'buffer'
          }

          //notes: slightly better speed with one transc

          const listofsqlrequests = [
            //start the series of transactions, don't save it until the end.
            
            //perform an alias lookup
            ` CREATE TABLE IF NOT EXISTS aliased${nameofidemp} AS (SELECT * from init${nameofidemp} LEFT JOIN aliastable ON init${nameofidemp}.vendor_name = aliastable.input);`,
            `DROP TABLE IF EXISTS losangelescheckbooknew;`,
            //create the real table
            ` CREATE TABLE IF NOT EXISTS losangelescheckbooknew AS (SELECT *,
              TRIM(REPLACE((CASE WHEN department_name like 'CIVIL, HUMAN RIGHTS, AND EQUITY DEPARTMENT' or department_name like 'CIVIL, HUMAN RIGHTS AND EQUITY DEPARTMENT' THEN 'CIVIL + HUMAN RIGHTS AND EQUITY' else department_name END), 'DEPARTMENT', '')) as department_name_new, 
              (case when settlement_judgment notnull then true else null end) as settlement_judgment_bool,
              DATE_PART('YEAR', transaction_date) as year ,
              (CASE  WHEN authority_link ILIKE '%http://cityclerk.lacity.org/lacityclerkconnect/index.cfm?%' THEN concat('c/', split_part(authority_link, '=', 3)) ELSE authority_link END) as authority_link_new,
              COALESCE ( showas,vendor_name ) as vendor_name_new FROM aliased${nameofidemp});`,

            //delete column vendor_name
           // `ALTER TABLE losangelescheckbooknew DROP COLUMN vendor_name;`,
            ` ALTER TABLE losangelescheckbooknew DROP COLUMN showas, DROP COLUMN input, DROP COLUMN settlement_judgment;`,

            //delete the old column authority_link
            //rename the column authority_link_new to authority_link

            ` ALTER TABLE losangelescheckbooknew RENAME COLUMN authority_link TO authority_link_original;`,
            ` ALTER TABLE losangelescheckbooknew RENAME COLUMN authority_link_new TO authority_link;`,

            `ALTER TABLE losangelescheckbooknew DROP COLUMN IF EXISTS department_name;`,
            `ALTER TABLE losangelescheckbooknew RENAME COLUMN department_name_new TO department_name;`,
              //now delete authority_link_original
            ` ALTER TABLE losangelescheckbooknew DROP COLUMN IF EXISTS authority_link_original,
             DROP COLUMN IF EXISTS bu_name, DROP COLUMN IF EXISTS department_number,
             DROP COLUMN IF EXISTS procurement_organization, DROP COLUMN IF EXISTS fiscal_year_quarter,
              DROP COLUMN IF EXISTS currency, DROP COLUMN IF EXISTS data_source;`,

            //rename the column vendor_name_new to vendor_name
            `ALTER TABLE losangelescheckbooknew RENAME COLUMN vendor_name TO vendor_name_original;`,
            `ALTER TABLE losangelescheckbooknew RENAME COLUMN vendor_name_new TO vendor_name;`,
            //rename the aliased table into losangelescheckbook
              //drop the init table
              
              `DROP TABLE IF EXISTS init${nameofidemp};`,
              `DROP TABLE IF EXISTS aliased${nameofidemp};`,
              //stacks of indexes go here
              ` CREATE INDEX losangelescheckbook_department_name_idx_${nameofidemp} ON losangelescheckbooknew USING BTREE (department_name);`,
              ` CREATE INDEX losangelescheckbook_vendor_name_btree_idx_${nameofidemp} ON losangelescheckbooknew USING BTREE (vendor_name);`,
              ` CREATE INDEX losangelescheckbook_vendor_name_idx_${nameofidemp} ON losangelescheckbooknew USING GIN(vendor_name);`,
              ` CREATE INDEX dateasc${nameofidemp} on losangelescheckbooknew USING BTREE (transaction_date asc);`,
              ` CREATE INDEX datedesc${nameofidemp} on losangelescheckbooknew USING BTREE (transaction_date desc);`,
              ` CREATE INDEX amountdesc${nameofidemp} on losangelescheckbooknew USING BTREE (dollar_amount desc);`,
              ` CREATE INDEX amountasc${nameofidemp} on losangelescheckbooknew USING BTREE (dollar_amount asc);`,
              ` CREATE INDEX deptdateasc${nameofidemp} on losangelescheckbooknew USING BTREE (department_name, transaction_date asc);`,
              ` CREATE INDEX deptdatedesc${nameofidemp} on losangelescheckbooknew USING BTREE (department_name, transaction_date desc);`,
              ` CREATE INDEX vendordateasc${nameofidemp} on losangelescheckbooknew USING BTREE (vendor_name, transaction_date asc);`,
              ` CREATE INDEX vendordatedesc${nameofidemp} on losangelescheckbooknew USING BTREE (vendor_name, transaction_date desc);`,
              ` CREATE INDEX vendordeptasc${nameofidemp} on losangelescheckbooknew USING BTREE (vendor_name, department_name asc);`,
              ` CREATE INDEX vendoramountasc${nameofidemp} on losangelescheckbooknew USING BTREE (vendor_name, dollar_amount asc);`,
              ` CREATE INDEX vendoramountdesc${nameofidemp} on losangelescheckbooknew USING BTREE (vendor_name, dollar_amount desc);`,
              ` CREATE INDEX vendordeptdesc${nameofidemp} on losangelescheckbooknew USING BTREE (vendor_name, department_name desc);`,
              ` CREATE INDEX accountdateasc${nameofidemp} on losangelescheckbooknew USING BTREE (account_name, transaction_date asc);`,
              ` CREATE INDEX accountdatedesc${nameofidemp} on losangelescheckbooknew USING BTREE (account_name, transaction_date desc);`,
              ` CREATE INDEX funddateasc${nameofidemp} on losangelescheckbooknew USING BTREE (fund_name, transaction_date asc);`,
              ` CREATE INDEX funddatedesc${nameofidemp} on losangelescheckbooknew USING BTREE (fund_name, transaction_date desc);`,
              ` CREATE INDEX programdateasc${nameofidemp} on losangelescheckbooknew USING BTREE (program, transaction_date asc);`,
              ` CREATE INDEX programdatedesc${nameofidemp} on losangelescheckbooknew USING BTREE (program, transaction_date desc);`
          ]


          executesqlarray(listofsqlrequests);
          console.log('cleaning up all the dropped columns');
          executesqlarray([`VACUUM FULL VERBOSE;`]);
          console.log('done vaccuming, swap tables')
          executesqlarray(['BEGIN;',
          //move the old table out of the way
          `ALTER TABLE losangelescheckbook RENAME TO losangelescheckbookold;`,
          //rename the new table to the old table
          `ALTER TABLE losangelescheckbooknew RENAME TO losangelescheckbook;`,
            //drop the old table
            `DROP TABLE losangelescheckbookold;`,
            //commit the action
           `COMMIT;`])

                //create the vendor lookup table

                console.log('main table done');
     
     
                console.log('make db update times');

                //make an sql table with columns lastuploaded (a datetime column), lastindexed (a datetime column), dbname (string), and filesize (bigint)

                const listofsqlrequestsmetainfo = [
                  "BEGIN;",
                  `CREATE TABLE IF NOT EXISTS dbupdateinfo (timeoffiledownload timestamp, lastuploaded timestamp, lastindexed timestamp, dbname varchar(255)  PRIMARY KEY, filesize bigint);`,
                  `INSERT INTO dbupdateinfo (timeoffiledownload, lastuploaded, dbname, filesize) VALUES ('${timeofdownload}', now(), 'checkbook', ${sizeoffile}) ON CONFLICT (dbname) DO UPDATE SET timeoffiledownload = '${timeofdownload}', lastuploaded = now(), filesize = ${sizeoffile};`,
                  `COMMIT;`
                ]
     
                executesqlarray(listofsqlrequestsmetainfo);

                console.log('making vendors summed');

             const listofsqlindexes = [
              "BEGIN;",
              `CREATE TABLE IF NOT EXISTS vendors_summed_${nameofidemp} AS (SELECT count(*), sum(dollar_amount), vendor_name FROM losangelescheckbook GROUP BY vendor_name ORDER BY SUM(dollar_amount) desc);`,
              `CREATE UNIQUE INDEX btree${shortIdempotency()} ON vendors_summed_${nameofidemp} (vendor_name);`,
              `CREATE INDEX gin_${shortIdempotency()} ON vendors_summed_${nameofidemp} USING GIN(vendor_name);`,
              `DROP TABLE IF EXISTS vendors_summed;`,
              `ALTER TABLE vendors_summed_${nameofidemp} RENAME TO vendors_summed;`,
              `COMMIT;`
             ]

             executesqlarray(listofsqlindexes);

             const witholdtablename = `vendors_summedwithold_${shortIdempotency()}`

             const listofsearchsqlindexes = [
              "BEGIN;",
              `CREATE TABLE IF NOT EXISTS ${witholdtablename} AS (SELECT count(*), sum(dollar_amount), vendor_name, vendor_name_original FROM losangelescheckbook GROUP BY (vendor_name,vendor_name_original) ORDER BY SUM(dollar_amount) desc);`,
              `CREATE INDEX ${shortIdempotency()} ON ${witholdtablename} USING GIN(vendor_name);`,
              `CREATE INDEX ${shortIdempotency()} ON ${witholdtablename} USING GIN(vendor_name_original);`,
              `DROP TABLE IF EXISTS vendors_summedwithold;`,
              `ALTER TABLE ${witholdtablename} RENAME TO vendors_summedwithold;`,
              `COMMIT;`
             ]

             executesqlarray(listofsearchsqlindexes);

             console.log('making search table for vendors');

             const searchvendorstable = `searchvendors_${shortIdempotency()}`

              const listofsearchsqlindexes2 = [
                "BEGIN;",
                `CREATE TABLE IF NOT EXISTS ${searchvendorstable} AS (SELECT vendor_name, SUM(dollar_amount), COUNT(*), array_agg( DISTINCT vendor_name_original) as vendor_old FROM losangelescheckbook GROUP BY vendor_name ORDER BY SUM(dollar_amount));`,
                `CREATE INDEX ${shortIdempotency()} ON ${searchvendorstable} USING GIN(vendor_name);`,
                `CREATE INDEX ${shortIdempotency()} ON ${searchvendorstable} USING GIN(vendor_old);`,
                `DROP TABLE IF EXISTS searchvendors;`,
                `ALTER TABLE ${searchvendorstable} RENAME TO searchvendors;`,
                `COMMIT;`
              ]

              executesqlarray(listofsearchsqlindexes2);
              console.log('done making vendor search table');

              console.log('making department summaries');

               const listofsqldeptindexes = [
                `BEGIN;`,
                `CREATE TABLE IF NOT EXISTS department_summary_new_${nameofidemp} AS (SELECT count(*), sum(dollar_amount), department_name FROM losangelescheckbook GROUP BY department_name ORDER BY SUM(dollar_amount) desc);`,
                `CREATE UNIQUE INDEX department_summary_department_name_idx_${nameofidemp} ON department_summary_new_${nameofidemp} (department_name);`,
                `DROP TABLE IF EXISTS department_summary;`,
                `ALTER TABLE department_summary_new_${nameofidemp} RENAME TO department_summary;`,
                `COMMIT;`
               ]

              executesqlarray(listofsqldeptindexes);

              console.log('making department by year summaries');

              const byyeardepttable = `department_summary_by_year_${shortIdempotency()}`

              const listofsqldeptbyyearindexes = [
               `BEGIN;`,
               `CREATE TABLE IF NOT EXISTS ${byyeardepttable} AS (SELECT count(*), sum(dollar_amount), department_name, year FROM losangelescheckbook GROUP BY (department_name, year) ORDER BY SUM(dollar_amount) desc);`,
               `CREATE INDEX ${shortIdempotency()} ON ${byyeardepttable} (department_name);`,
               `CREATE INDEX ${shortIdempotency()} ON ${byyeardepttable} USING GIN(department_name);`,
                `CREATE INDEX ${shortIdempotency()} ON ${byyeardepttable} (year);`,
               `DROP TABLE IF EXISTS department_summary_by_year;`,
               `ALTER TABLE ${byyeardepttable} RENAME TO department_summary_by_year;`,
               `COMMIT;`
              ]

             executesqlarray(listofsqldeptbyyearindexes);

                 console.log('making this year vendor summary')

              const thisyearvendorsummedrequests = [
                `CREATE TABLE latestyearpervendorsummarynew${nameofidemp} AS (SELECT count(*), sum(dollar_amount), vendor_name FROM losangelescheckbook WHERE date_part('year', transaction_date) = '${new Date().getFullYear()}' GROUP BY vendor_name ORDER BY SUM(dollar_amount) desc);`,
                
                `CREATE UNIQUE INDEX latestyearpervendorsummary_vendor_name_idx_${nameofidemp} ON latestyearpervendorsummarynew${nameofidemp} (vendor_name);`,
                `CREATE INDEX latestyearvendorsummary_gin_${nameofidemp} ON latestyearpervendorsummarynew${nameofidemp} USING GIN(vendor_name);`,
                `BEGIN;`,
                `DROP TABLE IF EXISTS latestyearpervendorsummary;`,
                `ALTER TABLE latestyearpervendorsummarynew${nameofidemp} RENAME TO latestyearpervendorsummary;`,
                `COMMIT;`
              ]
            

                 executesqlarray(thisyearvendorsummedrequests);

                 console.log('done making this year vendor summary')
                 
     
                  console.log('last postgres meta update info')
                 
                 const listofsqlrequestsmetainfo2 = [
                  "BEGIN;",
                  `CREATE TABLE IF NOT EXISTS dbupdateinfo (timeoffiledownload timestamp, lastuploaded timestamp, lastindexed timestamp, dbname varchar(255)  PRIMARY KEY, filesize bigint);`,
                  // update the time, not insert
                  `UPDATE dbupdateinfo SET lastindexed = now() WHERE dbname = 'checkbook';`,
                  `COMMIT;`
                ]
     
                executesqlarray(listofsqlrequestsmetainfo2);
               
                const checkbooktempnamerowcount = `checkbookrowsize${shortIdempotency()}`
                   
                const totalrowcount = [
                  "BEGIN;",
                  `CREATE TABLE IF NOT EXISTS ${checkbooktempnamerowcount} AS (SELECT count(*) FROM losangelescheckbook);`,
                  `DROP TABLE IF EXISTS checkbookrowsize;`,
                  `ALTER TABLE ${checkbooktempnamerowcount} RENAME TO checkbookrowsize;`,
                  `COMMIT;`,
                ]
     
                executesqlarray(totalrowcount);

                executesqlarray([`VACUUM;`])
               

            
  }
