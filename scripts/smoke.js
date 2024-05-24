import http from 'k6/http';
import { sleep } from 'k6';
import exec from 'k6/execution';
import encoding from 'k6/encoding';
import { randomString, randomItem, randomIntBetween, uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js'

export const options = {
    discardResponseBodies: true,
    scenarios: {
        contacts: {
            executor: 'shared-iterations',
            vus: 20,
            iterations: 6000,
        },
    },
};

function current_time() {
    const startDate = new Date('2024-05-17T15:00:00');
    const endDate = new Date('2024-05-17T15:03:59');
    const timeDiff = endDate.getTime() - startDate.getTime();
    const randomTime = Math.random() * timeDiff;
    const randomDate = new Date(startDate.getTime() + randomTime);
    return(randomDate.toISOString());
}

function schemas() {
    // fix schema count for smoke test
    return 8
}

const common_schema = [
    { "name": "source_time", "gen": current_time, "arg": null },
    { "name": "level", "gen": randomItem, "arg": ["info", "warn", "error"] },
    { "name": "message", "gen": randomItem, "arg": ["Application started", "Application is failing", "Logging a request"] },
    { "name": "version", "gen": randomItem, "arg": ["1.0.0", "1.1.0", "1.2.0"] },
    { "name": "user_id", "gen": randomIntBetween, "arg": [10000, 100000] },
    { "name": "device_id", "gen": randomIntBetween, "arg": [0, 5000] },
    { "name": "session_id", "gen": randomItem, "arg": ["abc", "pqr", "xyz"] },
    { "name": "os", "gen": randomItem, "arg": ["macOS", "Linux", "Windows"] },
    { "name": "host", "gen": randomItem, "arg": ["192.168.1.100", "112.168.1.110", "172.162.1.120"] },
    { "name": "uuid", "gen": uuidv4, "arg": null },
]

const add_fields = {
    "location": { "gen": randomString, "arg": 16 },
    "timezone": { "gen": randomString, "arg": 3 },
    "user_agent": { "gen": randomItem, "arg": ["Banana", "PineApple", "PearOS", "OrangeOS", "Kiwi"] },
    "runtime": { "gen": randomString, "arg": 3 },
    "request_body": { "gen": randomString, "arg": 100 },
    "status_code": { "gen": randomItem, "arg": [200, 300, 400, 500] },
    "response_time": { "gen": randomItem, "arg": [12, 22, 34, 56, 70, 112] },
    "process_id": { "gen": randomIntBetween, "arg": [100, 1000] },
    "app_meta": { "gen": randomString, "arg": 24 }
}

const addFields_permutation = [
    ['location', 'request_body', 'status_code', 'app_meta'],
    ['timezone', 'user_agent', 'runtime', 'app_meta'],
    ['timezone', 'request_body', 'response_time', 'process_id'],
    ['timezone', 'user_agent', 'request_body', 'process_id'],
    ['runtime', 'status_code', 'response_time', 'process_id'],
    ['location', 'user_agent', 'runtime', 'process_id'],
    ['location', 'timezone', 'request_body', 'response_time'],
    ['timezone', 'user_agent', 'status_code', 'process_id'],
    ['timezone', 'runtime', 'request_body', 'response_time'],
    ['timezone', 'status_code', 'response_time', 'process_id'],
    ['timezone', 'runtime', 'status_code', 'response_time'],
    ['location', 'timezone', 'response_time', 'process_id'],
    ['location', 'timezone', 'runtime', 'process_id'],
    ['user_agent', 'runtime', 'status_code', 'process_id'],
    ['timezone', 'response_time', 'process_id', 'app_meta'],
    ['location', 'user_agent', 'status_code', 'response_time'],
    ['timezone', 'user_agent', 'runtime', 'status_code'],
    ['request_body', 'status_code', 'process_id', 'app_meta'],
    ['location', 'user_agent', 'runtime', 'request_body'],
    ['location', 'timezone', 'status_code', 'response_time'],
    ['location', 'user_agent', 'response_time', 'process_id'],
    ['timezone', 'runtime', 'response_time', 'process_id'],
    ['location', 'timezone', 'user_agent', 'runtime'],
    ['user_agent', 'request_body', 'status_code', 'process_id'],
    ['runtime', 'request_body', 'response_time', 'process_id'],
    ['location', 'runtime', 'request_body', 'app_meta'],
    ['runtime', 'response_time', 'process_id', 'app_meta'],
    ['location', 'runtime', 'status_code', 'app_meta'],
    ['location', 'runtime', 'process_id', 'app_meta'],
    ['location', 'request_body', 'process_id', 'app_meta'],
    ['location', 'timezone', 'runtime', 'request_body'],
    ['timezone', 'user_agent', 'response_time', 'app_meta'],
    ['runtime', 'request_body', 'status_code', 'response_time'],
    ['location', 'timezone', 'user_agent', 'response_time'],
    ['location', 'runtime', 'request_body', 'status_code'],
    ['location', 'user_agent', 'request_body', 'response_time'],
    ['location', 'status_code', 'process_id', 'app_meta'],
    ['user_agent', 'status_code', 'response_time', 'app_meta'],
    ['timezone', 'request_body', 'status_code', 'response_time'],
    ['user_agent', 'runtime', 'request_body', 'process_id'],
    ['user_agent', 'runtime', 'response_time', 'app_meta'],
    ['user_agent', 'request_body', 'response_time', 'app_meta']
];

function generateOverlappingSchemas(number = 5) {
    const schemas = [];
    addFields_permutation.slice(0, number).forEach((listOfFields) => {
        const new_schema = [...common_schema];
        listOfFields.forEach((field) => {
            let gen_value = add_fields[field];
            let obj = { "name": field };
            Object.assign(obj, gen_value);
            new_schema.push(obj)
        })
        schemas.push(new_schema)
    })

    return schemas;
}

function generateJSON(schema) {
    let json = {};
    schema.forEach(item => {
        let { name, gen, arg } = item;
        var value;
        if ((gen === current_time) || (gen === uuidv4)) {
            value = gen();
        }
        else if (gen === randomIntBetween) {
            value = gen(...arg);
        }
        else {
            value = gen(arg)
        }
        json[name] = value;
    });
    return json;
}

function generateEvents(num = 5) {
    const events = [];
    let numberOfSchemas = schemas();
    let listOfSchema = generateOverlappingSchemas(numberOfSchemas);

    // generate event of random schema from the list
    for (let index = 0; index < num; index++) {
        events.push(generateJSON(listOfSchema[Math.floor(Math.random() * listOfSchema.length)]));
    }

    return events
}

export default function () {
    sleep(1);
    const url = `${__ENV.P_URL}/api/v1/ingest`;
    const credentials = `${__ENV.P_USERNAME}:${__ENV.P_PASSWORD}`;
    const encodedCredentials = encoding.b64encode(credentials);

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'Basic ' + `${encodedCredentials}`,
            'X-P-STREAM': `${__ENV.P_STREAM}`,
            'X-P-META-Source': 'quest-smoke-test',
            'X-P-META-Test': 'Fixed-Logs',
        }
    }

    let events = generateEvents(5);

    // send all events batched
    let batched_request = JSON.stringify(events);
    let response = http.post(url, batched_request, params);

    if (response.status != 200) {
        exec.test.abort("Failed to send event.. status != 200");
    }

    // send a request per event
    let responses = http.batch(events.map(event => ['POST', url, JSON.stringify(event), params]))

    if (responses.some(res => res.status != 200)) {
        exec.test.abort("Failed to send event.. status != 200");
    }
}
