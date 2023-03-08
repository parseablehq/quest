import http from 'k6/http';
import { check, fail, sleep } from 'k6';
import encoding from 'k6/encoding';
import { randomString, randomItem, randomIntBetween, uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js'

export const options = {
    discardResponseBodies: true,
    scenarios: {
        contacts: {
            executor: 'constant-vus',
            vus: 10,
            duration: '60s',
        },
    },
};

function current_time() {
    let event = new Date();
    return event.toISOString();
}

function payload1() {
    return JSON.stringify([{
        "source_time": current_time(),
        "level": randomItem["info", "warn", "error"],
        "message": randomItem(["Application started", "Application is failing", "Logging a request"]),
        "version": "1.0.0",
        "user_id": randomIntBetween(10000, 100000),
        "device_id": randomIntBetween(0, 5000),
        "session_id": randomItem(["abc", "pqr", "xyz"]),
        "os": randomItem(["macOS", "Linux", "Windows"]),
        "host": "192.168.1.100",
        "location": randomString(16),
        "timezone": "PST",
        "URL": "www.example.com",
        "user_agent": randomItem(["Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "AppleWebKit/537.36", "Chrome/89.0.4389.82", "Safari/537.36"]),
        "request_body": randomString(100)
    }])
}


function payload2() {
    return JSON.stringify([{
        "source_time": current_time(),
        "level": randomItem["info", "warn", "error"],
        "message": randomItem(["Service started", "Service restarted", "Service Stopped"]),
        "uuid": uuidv4(),
        "response_time": randomItem([10, 20, 30, 40]),
        "status_code": randomItem([200, 300, 401, 403, 404, 413]),
        "error_message": randomString(24),
        "process_id": randomIntBetween(100, 1000),
        "runtime": "xyz",
        "runtime_version": "v0.1.1",
        "app_meta": randomString(24)
    }])
}


function payload3() {
    return JSON.stringify([{
        "source_time": current_time(),
        "version": "1.0.0",
        "device_id": randomIntBetween(0, 5000),
        "os": randomItem(["macOS", "Linux", "Windows"]),
        "request_id": randomString(24),
        "response_time": randomItem([10, 20, 30, 40]),
        "status_code": randomItem([200, 300, 401, 403, 404, 413]),
        "runtime": "xyz",
        "runtime_version": "v0.1.1",
        "app_meta": randomString(24),
        "user_agent": randomItem(["Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "AppleWebKit/537.36", "Chrome/89.0.4389.82", "Safari/537.36"]),
        "request_body": randomString(100)
    }])
}


export default function () {
    sleep(0.1);
    const url = `${__ENV.P_URL}/api/v1/ingest`;
    const credentials = `${__ENV.P_USERNAME}:${__ENV.P_PASSWORD}`;
    const encodedCredentials = encoding.b64encode(credentials);

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'Basic '+`${encodedCredentials}`,
            'X-P-STREAM': `${__ENV.P_STREAM}`,
            'X-P-META-Host': '10.116.0.3',
            'X-P-META-Source': 'quest-test',
        }
    }

    http.post(url, payload1(), params);
    http.post(url, payload2(), params);
    http.post(url, payload3(), params);
}