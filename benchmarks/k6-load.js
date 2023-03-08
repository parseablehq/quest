import http from 'k6/http';
import { check, fail, sleep } from 'k6';
import { randomString, randomItem, randomIntBetween, uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js'

export const options = {
    discardResponseBodies: true,
    scenarios: {
        contacts: {
            executor: 'constant-vus',
            vus: 10,
            duration: '3600s',
        },
    },
};

function current_time() {
    let event = new Date();
    return event.toISOString();
}

function payload(id) {
    let partial_payload = {
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
    };
    
    partial_payload["columns_"+id] = randomString(24)
    partial_payload["columns_as_"+id] = randomString(24)
    
    return JSON.stringify([partial_payload]);
}

export default function () {
    sleep(0.01);
    const url = 'http://localhost:8000/api/v1/ingest';

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'Basic YWRtaW46YWRtaW4=',
            'X-P-STREAM': "app",
            'X-P-META-Host': '10.116.0.3',
            'X-P-META-Source': '10.244.0.147',
            'X-P-META-ContainerName': 'log-generator',
            'X-P-META-ContainerImage': 'mingrammer/flog',
            'X-P-META-PodName': 'go-app-6c87bc9cc9-vqv66',
            'X-P-META-Namespace': 'go-apasdp',
            'X-P-META-PodLabels': 'app=go-app,pod-template-hash=6c87bc9cc9',
        }
    }

    for (let i = 0; i < 10; i++) {
        http.post(url, payload(i), params);
    }
}
