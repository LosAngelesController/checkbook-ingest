const execSync = require('child_process').execSync;

var randomstring = require("randomstring");
// import { execSync } from 'child_process';  // replace ^ if using ES modules

const config = require('./config.json');

const setup = ` PGPASSWORD=${config.password} psql "sslmode=verify-ca sslrootcert=server-ca.pem sslcert=client-cert.pem sslkey=client-key.pem hostaddr=${config.hostname} port=5432 user=${config.username} dbname=postgres"`




          function executesqlarray(sqlarray:Array<string>) {
            execSync(
              `${setup}  -c "${sqlarray.join('')}"`, 
               { encoding: 'utf-8',
               stdio: 'inherit'});  // the default is 'buffer'
          }

            executesqlarray(["SELECT GETDATE()"]);
