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
execSync('if [ -d inputs ]; then rm -rf inputs; fi', { encoding: 'utf-8' });
const makenewfolder = execSync('[ -d inputs ] || mkdir inputs', { encoding: 'utf-8' });  // the default is 'buffer'
console.log('Output was:\n', makenewfolder);



console.log('making alias table');


var listofalias = [
  'DROP TABLE IF EXISTS aliastable;', // drop the table if it exists
  // ensure the table at least exists, if not, create it
'CREATE TABLE IF NOT EXISTS aliastable (input varchar(255) PRIMARY KEY,showas varchar(255));',
//insert the alias into the table
`INSERT INTO aliastable (input, showas) VALUES ('MICROSOFT CORP', 'MICROSOFT CORPORATION') ON CONFLICT DO NOTHING;`,
"CREATE EXTENSION IF NOT EXISTS pg_trgm;CREATE EXTENSION IF NOT EXISTS btree_gin;"
]

const setup = ` PGPASSWORD=${config.password} psql "sslmode=verify-ca sslrootcert=server-ca.pem sslcert=client-cert.pem sslkey=client-key.pem hostaddr=${config.hostname} port=5432 user=${config.username} dbname=postgres"`


if (true){
const output = execSync(
    'wget https://controllerdata.lacity.org/api/views/pggv-e4fn/rows.csv?accessType=DOWNLOAD -O '
     + csvname, 
     { encoding: 'utf-8',
     stdio: 'inherit'});  // the default is 'buffer'
}



          function executesqlarray(sqlarray:Array<string>) {
            execSync(
              `${setup}  -c "${sqlarray.join('')}"`, 
               { encoding: 'utf-8',
               stdio: 'inherit'});  // the default is 'buffer'
          }

            executesqlarray(["SELECT GETDATE()"]);
