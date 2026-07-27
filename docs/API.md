# QueueLine API Documentation

This document describes the REST API endpoints provided by the QueueLine service.

## Base URL
By default, the API is served at `http://localhost:8080`.

## 1. Queues and Jobs

### 1.1 Enqueue Job
`POST /v1/queues/{queue}/jobs`

Enqueues a new job onto the specified queue.

**Request Body:**
```json
{
  "payload": { "key": "value" }, // Required: Any valid JSON object representing the job data
  "priority": 1,                 // Optional: Higher numbers mean higher priority (default 0)
  "delaySeconds": 0,             // Optional: Number of seconds to delay the job execution
  "maxAttempts": 3,              // Optional: Maximum number of attempts before dead-lettering (default 5)
  "dedupKey": "unique-id"        // Optional: Idempotency key. If a job with this key already exists, returns the existing job.
}
```
**Responses:**
- `201 Created`: Job was successfully created.
- `200 OK`: A job with this `dedupKey` already existed and was returned.

### 1.2 Lease Job
`POST /v1/queues/{queue}/lease`

Claims the oldest, highest priority pending job from the queue. Provides a fencing token (`leaseId`) that must be used for further operations on this job.

**Request Body:**
```json
{
  "leaseSeconds": 30 // Optional: The number of seconds the lease is valid for.
}
```
**Responses:**
- `200 OK`: Job claimed. Contains the `leaseId` and `leaseExpiresAt`.
- `204 No Content`: The queue is empty. No job to claim.

### 1.3 Queue Stats
`GET /v1/queues/{queue}/stats`

Returns statistics for the specified queue.

**Responses:**
- `200 OK`: Returns the count of pending, leased, dead-lettered jobs, and jobs completed in the last 24h.

---

## 2. Job Lifecycle (Worker Operations)

All worker operations require the `leaseId` returned by the `Lease Job` endpoint.

### 2.1 Heartbeat Job
`POST /v1/jobs/{id}/heartbeat`

Extends the lease of a job to prevent the reaper from claiming it.

**Request Body:**
```json
{
  "leaseId": "string",       // Required: The fencing token
  "extendSeconds": 30        // Optional: Seconds to extend the lease by
}
```
**Responses:**
- `204 No Content`: Lease successfully extended.
- `409 Conflict`: The lease has expired and been reclaimed, or the `leaseId` is incorrect.

### 2.2 Complete Job
`POST /v1/jobs/{id}/complete`

Marks a job as successfully completed.

**Request Body:**
```json
{
  "leaseId": "string" // Required: The fencing token
}
```
**Responses:**
- `204 No Content`: Job marked as completed.
- `409 Conflict`: The lease mismatch.

### 2.3 Fail Job
`POST /v1/jobs/{id}/fail`

Marks a job attempt as failed. If attempts are exhausted, the job is moved to dead-letters. Otherwise, it is rescheduled with exponential backoff.

**Request Body:**
```json
{
  "leaseId": "string",           // Required: The fencing token
  "error": "Error description"   // Optional: Description of the failure
}
```
**Responses:**
- `204 No Content`: Job failure recorded.
- `409 Conflict`: Lease mismatch.

### 2.4 Get Job
`GET /v1/jobs/{id}`

Retrieves the current state and details of a job.

**Responses:**
- `200 OK`: Job details.
- `404 Not Found`: Job does not exist.

---

## 3. Dead Letters

### 3.1 List Dead Letters
`GET /v1/queues/{queue}/dead-letters`

Lists up to the latest 100 dead-lettered jobs for a specific queue.

**Responses:**
- `200 OK`: Array of dead-letter jobs.

### 3.2 Requeue Dead Letter
`POST /v1/dead-letters/{id}/requeue`

Re-enqueues a dead-lettered job, creating a new `PENDING` job with attempts reset. The old dead letter record is preserved.

**Responses:**
- `201 Created`: The newly enqueued job.
- `404 Not Found`: Dead letter not found.

---

## 4. Observability

### 4.1 Health Check
`GET /healthz`
Returns `200 OK` if the HTTP server is running.

### 4.2 Readiness Check
`GET /readyz`
Returns `200 OK` if the server is running AND can reach the PostgreSQL database. Returns `503 Service Unavailable` otherwise.

### 4.3 Metrics
`GET /metrics`
Exposes Prometheus metrics.
