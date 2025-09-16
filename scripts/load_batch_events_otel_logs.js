import http from 'k6/http';
import { check, sleep } from 'k6';
import exec from 'k6/execution';
import encoding from 'k6/encoding';
import { randomString, randomItem, randomIntBetween, uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

// config for load test, uncomment to perform load test for an hour
// export const options = {
//     discardResponseBodies: true,
//     // Key configurations for avg load test in this section
//     stages: [
//       { duration: '5m', target: 10 },
//       { duration: '60m', target: 8 },
//       { duration: '5m', target: 0 },
//     ],
// };

// default options for all tests
export const options = {
    discardResponseBodies: true,
    scenarios: {
        contacts: {
            executor: 'constant-vus',
            vus: 10,
            duration: "5m",
        },
    },
};

// Helper function to generate current ISO time
function currentIsoTime() {
    let event = new Date();
    return event.toISOString();
}

// Helper function to generate unix nano time
function currentUnixNanoTime() {
    return new Date().toISOString();
}

// Helper to generate trace ID (hex string)
function generateTraceId() {
    return [...Array(32)].map(() => Math.floor(Math.random() * 16).toString(16)).join('');
}

// Helper to generate span ID (hex string)
function generateSpanId() {
    return [...Array(16)].map(() => Math.floor(Math.random() * 16).toString(16)).join('');
}

// Generate HTTP methods
function randomHttpMethod() {
    return randomItem(["GET", "POST", "PUT", "DELETE", "PATCH"]);
}

// Generate API endpoints
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
    
    // Replace {id} with random product/user ID if present
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

// Generate IP addresses
function randomIpAddress() {
    return `172.18.0.${randomIntBetween(1, 40)}`;
}

// Generate port number
function randomPort() {
    return randomIntBetween(8000, 60000);
}

function randomStatusCode() {
    return randomItem([200, 206, 304, 400, 404, 500, 503]);
}

// Map status code to appropriate severity number and text
function getSeverityFromStatusCode(statusCode) {
    // Define severity mapping based on status code ranges
    if (statusCode >= 100 && statusCode < 200) {
        // 1xx - Informational
        return { number: randomItem([9, 10, 11, 12]), text: "INFO" };
    } else if (statusCode >= 200 && statusCode < 300) {
        // 2xx - Success
        return { number: randomItem([9, 10, 11, 12]), text: "INFO" };
    } else if (statusCode >= 300 && statusCode < 400) {
        // 3xx - Redirection
        return { number: randomItem([9, 10, 11, 12]), text: "INFO" };
    } else if (statusCode >= 400 && statusCode < 500) {
        // 4xx - Client Error
        return { number: randomItem([13, 14, 15, 16]), text: "WARN" };
    } else if (statusCode >= 500) {
        // 5xx - Server Error
        const severityRoll = Math.random();
        if (severityRoll < 0.7) {
            return { number: randomItem([17, 18, 19, 20]), text: "ERROR" };
        } else {
            return { number: randomItem([21, 22, 23, 24]), text: "FATAL" };
        }
    } else {
        // Default or unknown
        return { number: 0, text: "SEVERITY_NUMBER_UNSPECIFIED" };
    }
}

// Generate random user agent
function randomUserAgent() {
    const userAgents = [
        "python-requests/2.32.3",
        "curl/7.88.1",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 15_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
        "PostmanRuntime/7.29.0"
    ];
    return randomItem(userAgents);
}

// Generate service names
function randomServiceName() {
    return randomItem([
        "frontend-proxy", 
        "product-service",
        "cart-service", 
        "user-service", 
        "checkout-service",
        "recommendation-service",
        "auth-service"
    ]);
}

// Generate body content for log 
function generateLogBody(method, path, statusCode, userAgent) {
    const timestamp = currentIsoTime();
    const bytesSent = randomIntBetween(100, 2000);
    const duration = randomIntBetween(1, 100);
    const durationMs = randomIntBetween(1, 20);
    const requestId = uuidv4().replace(/-/g, '-');
    const upstreamAddress = `${randomIpAddress()}:8080`;
    const sourceAddress = randomIpAddress();
    const destAddress = randomIpAddress();
    const sourcePort = randomPort();
    const destPort = 8080;
    const clientPort = randomPort();
    
    return `[${timestamp}] "${method} ${path} HTTP/1.1" ${statusCode} - via_upstream - "-" 0 ${bytesSent} ${duration} ${durationMs} "-" "${userAgent}" "${requestId}" "frontend-proxy:8080" "${upstreamAddress}" frontend ${sourceAddress}:${sourcePort} ${destAddress}:${destPort} ${sourceAddress}:${clientPort} - -\n`;
}

// Generate a single OTEL log entry
function generateOtelLog() {
    const method = randomHttpMethod();
    const path = randomApiEndpoint();
    const statusCode = randomStatusCode();
    const userAgent = randomUserAgent();
    const serviceName = randomServiceName();
    const traceId = randomId(32);
    const spanId = randomId(16);
    const timeUnixNano = currentUnixNanoTime();
    const destAddress = randomIpAddress();
    const serverAddress = randomIpAddress();
    const sourceAddress = randomIpAddress();
    const upstreamHost = randomIpAddress();
    
    // Get severity based on status code
    const severity = getSeverityFromStatusCode(statusCode);
    
    return {
        "body": generateLogBody(method, path, statusCode, userAgent),
        "observed_time_unix_nano": timeUnixNano, // Using the same value as time_unix_nano
        "destination.address": destAddress,
        "event.name": "proxy.access",
        "server.address": serverAddress,
        "source.address": sourceAddress,
        "upstream.cluster": randomItem(["frontend", "backend", "auth", "product"]),
        "upstream.host": upstreamHost,
        "user_agent.original": randomUserAgent(),
        "service.name": serviceName,
        "severity_number": severity.number,
        "severity_text": severity.text,
        "span_id": spanId,
        "time_unix_nano": timeUnixNano,
        "trace_id": traceId,
        "url.full": `http://${serviceName}:8080${path}`,
        "url.path": path,
    };
}

// Generate a batch of OTEL logs
function generateOtelLogs(count) {
    const logs = [];
    for (let i = 0; i < count; i++) {
        logs.push(generateOtelLog());
    }
    return logs;
}

// Get events per call from environment variable
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
            'X-P-META-Host': '10.116.0.3',
            'X-P-META-Source': 'otel-logs-generator'
        }
    };

    let events = eventsPerCall();
    
    let batchRequests = JSON.stringify(generateOtelLogs(events));
    
    let response = http.post(url, batchRequests, params);
    let date = new Date();
    
    if (!check(response, {
        'status code MUST be 200': (res) => res.status == 200,
    })) {
        console.log(`Time: ${date}, Response: ${response.status}`);
    }
}