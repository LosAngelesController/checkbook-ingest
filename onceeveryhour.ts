import {main} from './ingest'

console.log('starting first ingest at ', new Date());

main();

console.log('finished first ingest at', new Date());

setInterval(() => {
    console.log('starting ingest at ', new Date());
    main();
    console.log('finished ingest at', new Date());
}, 3600_000);