# Comparative performance analysis with Elastic

For the record, Elastic is much more matured software and has been around for a long time. They have a lot of experience in this domain and have been able to optimize their software to the best possible level. We are not trying to compete with them, but rather to provide a comparison of the performance of Parseable with Elastic.

Additionally, Elastic is a distributed system allowing cluster scaling. While, Parseable is a single node system as of now. We are working on a distributed version of Parseable and will update this section with the results. With these caveats in mind, let's look at the performance comparison.

### Elastic

Currently we base our comparison with Elastic on their public benchmarks published here: [https://www.elastic.co/blog/benchmarking-and-sizing-your-elasticsearch-cluster-for-logs-and-metrics](https://www.elastic.co/blog/benchmarking-and-sizing-your-elasticsearch-cluster-for-logs-and-metrics).

As per this benchmark, Elastic is able to ingest 22000 events per second per node. Node specs: 8 vCPU, 32 GiB RAM.

### Parseable

We deployed a single node with 4vCPU, 16 GiB RAM (i.e. 50% CPU and Memory). For load generation we used [K6](https://k6.io). With this server, Parseable is able to ingest ~28000 events per second, while memory usage at all times in the server was 25% (max 4 GiB). This indicates 50% less CPU and up to 80% less memory footprint.

The load generation setup included two physical machines with 800 clients per machine (total 1600 clients). Combined these clients were able to push 28000 events per second.

```bash
running (1m00.1s), 000/800 VUs, 420535 complete and 0 interrupted iterations  
default ✓ [======================================] 800 VUs  1m0s  
  
data_received..................: 192 MB  3.2 MB/s  
data_sent......................: 757 MB  13 MB/s  
http_req_blocked...............: avg=53.06µs  min=0s      med=3.99µs   max=1.13s    p(90)=6.68µs   p(95)=8.12µs  
http_req_connecting............: avg=47.96µs  min=0s      med=0s       max=1.04s    p(90)=0s       p(95)=0s  
http_req_duration..............: avg=37.75ms  min=2.96ms  med=35.77ms  max=323.91ms p(90)=48.25ms  p(95)=54.61ms  
{ expected_response:true }...: avg=37.75ms  min=2.96ms  med=35.77ms  max=323.91ms p(90)=48.25ms  p(95)=54.61ms  
http_req_failed................: 0.00%   ✓ 0            ✗ 1261605  
http_req_receiving.............: avg=48.71µs  min=9.36µs  med=21.77µs  max=165.59ms p(90)=30.99µs  p(95)=51µs  
http_req_sending...............: avg=28.85µs  min=5.8µs   med=16.66µs  max=159ms    p(90)=25.75µs  p(95)=35.12µs  
http_req_tls_handshaking.......: avg=0s       min=0s      med=0s       max=0s       p(90)=0s       p(95)=0s  
http_req_waiting...............: avg=37.67ms  min=2.91ms  med=35.71ms  max=310.31ms p(90)=48.15ms  p(95)=54.39ms  
http_reqs......................: 1261605 20996.427837/s  
iteration_duration.............: avg=114.07ms min=37.32ms med=107.49ms max=1.31s    p(90)=141.88ms p(95)=153.58ms  
iterations.....................: 420535  6998.809279/s  
vus............................: 800     min=800        max=800  
vus_max........................: 800     min=800        max=800
```

```bash
running (1m00.1s), 000/800 VUs, 141384 complete and 0 interrupted iterations  
default ✓ [======================================] 800 VUs  1m0s  
  
data_received..................: 65 MB  1.1 MB/s  
data_sent......................: 255 MB 4.2 MB/s  
http_req_blocked...............: avg=92.24µs  min=1.28µs   med=4.06µs  max=148.3ms  p(90)=5.01µs   p(95)=5.61µs  
http_req_connecting............: avg=86.1µs   min=0s       med=0s      max=148.25ms p(90)=0s       p(95)=0s  
http_req_duration..............: avg=112.84ms min=441.39µs med=29.59ms max=59.46s   p(90)=39.35ms  p(95)=42.01ms  
{ expected_response:true }...: avg=112.84ms min=441.39µs med=29.59ms max=59.46s   p(90)=39.35ms  p(95)=42.01ms  
http_req_failed................: 0.00%  ✓ 0           ✗ 424152  
http_req_receiving.............: avg=24.51µs  min=10.54µs  med=20.66µs max=55.71ms  p(90)=30.61µs  p(95)=33.86µs  
http_req_sending...............: avg=42.18µs  min=10.84µs  med=15.68µs max=40.16ms  p(90)=24.57µs  p(95)=27.67µs  
http_req_tls_handshaking.......: avg=0s       min=0s       med=0s      max=0s       p(90)=0s       p(95)=0s  
http_req_waiting...............: avg=112.78ms min=406.97µs med=29.54ms max=59.46s   p(90)=39.3ms   p(95)=41.96ms  
http_reqs......................: 424152 7062.427709/s  
iteration_duration.............: avg=339.23ms min=4.06ms   med=90.44ms max=59.65s   p(90)=118.18ms p(95)=125.43ms  
iterations.....................: 141384 2354.14257/s  
vus............................: 800    min=800       max=800  
vus_max........................: 800    min=800       max=800
```

NOTE: Benchmarks are nuanced and very much environment specific. So we recommend running benchmarks in the target environment to get an understanding of actual performance.

### Test Parseable with K6

We have created a [K6](https://k6.io) script to test Parseable. You can use this script to test Parseable in your environment. The script is available [here](./k6-load.js).

#### Pre-requisites

* [K6](https://k6.io) installed.
* [Parseable](https://parseable.io) installed and running.

#### Start the script

```sh
k6 run k6-load.js
```
