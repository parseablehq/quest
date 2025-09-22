import http from 'k6/http';
import encoding from 'k6/encoding';
import { check } from 'k6';

// config for load test, uncomment to perform load test for an hour
export const options = {
    discardResponseBodies: true,
    // Key configurations for avg load test in this section
    stages: [
      { duration: '5m', target: 10 },
      { duration: '60m', target: 8 },
      { duration: '5m', target: 0 },
    ],
};

// default options for all tests
// export const options = {
//     discardResponseBodies: true,
//     scenarios: {
//         contacts: {
//             executor: 'constant-vus',
//             vus: 10,
//             duration: "5m",
//         },
//     },
// };

function randomIntBetween(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomItem(arr) {
    return arr[randomIntBetween(0, arr.length - 1)];
}

function randomId(len = 16) {
    return [...Array(len)].map(() => Math.floor(Math.random() * 16).toString(16)).join('');
}

function randomHttpMethod() {
    return randomItem(["GET", "POST", "PUT", "DELETE", "PATCH"]);
}

function randomApiEndpoint() {
    const endpoints = [
        "/api/products/{id}",
        "/api/cart",
        "/api/checkout",
        "/api/users/{id}",
        "/api/orders/{id}",
        "/api/recommendations",
        "/api/categories",
        "/api/search",
        "/api/auth/login",
        "/api/auth/logout"
    ];
    let endpoint = randomItem(endpoints);
    if (endpoint.includes("{id}")) {
        const id = randomItem([
            "0PUK6V6EV0", "1YMWWN1N4O", "2ZYFJ3GM2N", 
            "66VCHSJNUP", "6E92ZMYYFZ", "9SIQT8TOJO", 
            "L9ECAV7KIM", "LS4PSXUNUM", "OLJCESPC7Z"
        ]);
        endpoint = endpoint.replace("{id}", id);
    }
    return endpoint;
}

function randomStatusCode() {
    return randomItem([200, 206, 304, 400, 404, 500, 503]);
}

function randomServiceName() {
    return randomItem([
        "frontend", 
        "product-service",
        "cart-service", 
        "user-service", 
        "checkout-service",
        "recommendation-service",
        "auth-service"
    ]);
}

function randomEventName() {
    return randomItem([
        "message",
        "Product Found",
        "ResponseReceived",
        "Sent",
        "Enqueued"
    ]);
}

function random_availability_zone() {
    return randomItem([
        "us-east-1a",
        "us-east-1b",
        "us-east-1c",
        "us-east-1d"
    ]);
}


function generateTraceRecord() {
    const method = randomHttpMethod();
    const endpoint = randomApiEndpoint();
    const statusCode = randomStatusCode();
    const serviceName = randomServiceName();
    const traceId = randomId(32);
    const spanId = randomId(16);
    const now = new Date();
    const startTime = new Date(now.getTime() - 2 * 60 * 1000).toISOString(); // 2 minutes before now
    const endTime = now.toISOString(); // now
    const eventTime = now.toISOString(); // now
    const eventName = randomEventName();
    const availability_zone = random_availability_zone();
    return {
        "cloud.account.id": "724973952305",
        "cloud.availability_zone": availability_zone,
        "cloud.platform": "aws_ec2",
        "cloud.provider": "aws",
        "cloud.region": "us-east-1",
        "container.id": randomId(64),
        "docker.cli.cobra.command_path": "docker compose",
        "host.arch": "amd64",
        "host.id": "i-0c4b2f3eEXAMPLE",
        "host.name": "ip-172-31-33-209.ec2.internal",
        "http.method": method,
        "http.status_code": statusCode,
        "http.target": endpoint,
        "os.type": "linux",
        "os.version": "6.14.0-1011-aws",
        "process.command": "/app/server.js",
        "process.executable.path": "/nodejs/bin/node",
        "process.pid": randomIntBetween(1, 10000).toString(),
        "process.runtime.name": "nodejs",
        "process.runtime.version": "22.17.1",
        "service.name": serviceName,
        "span_end_time_unix_nano": endTime,
        "span_name": `${method} ${endpoint}`,
        "span_span_id": spanId,
        "span_start_time_unix_nano": startTime,
        "span_status_code": statusCode,
        "span_trace_id": traceId,
        "telemetry.sdk.language": "nodejs",
        "telemetry.sdk.name": "opentelemetry",
        "telemetry.sdk.version": "2.0.1",
        "event_time_unix_nano": eventTime,
        "event_name": eventName
    };
}

// Generate a batch of OTEL traces
function generateOtelTraces(count) {
    const traces = [];
    for (let i = 0; i < count; i++) {
        traces.push(generateTraceRecord());
    }
    return traces;
}

function eventsPerCall() {
    return Number(__ENV.P_EVENTS_COUNT) || 100;
}

export default function () {
    const url = `${__ENV.P_URL}/api/v1/ingest`;
    const credentials = `${__ENV.P_USERNAME}:${__ENV.P_PASSWORD}`;
    const encodedCredentials = encoding.b64encode(credentials);

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'Basic ' + `${encodedCredentials}`,
            'X-P-STREAM': `${__ENV.P_STREAM}`,
            'X-P-Telemetry-Type': 'traces',
            'X-P-META-Host': '10.116.0.3',
            'X-P-META-Source': 'otel-traces-generator'
        }
    };

    let events = eventsPerCall();
    let batchRequests = JSON.stringify(generateOtelTraces(events));
    let response = http.post(url, batchRequests, params);
    let date = new Date();

    if (!check(response, {
        'status code MUST be 200': (res) => res.status == 200,
    })) {
        console.log(`Time: ${date}, Response: ${response.status}`);
    }
}
