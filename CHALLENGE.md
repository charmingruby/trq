# Challenge

> Note: The details below were provided by someone who was interviewed. They represent the requirements and context given during the interview, rather than assumptions made afterward.

The coding round asked me to implement a **work queue** and the main operations required to manage jobs through their lifecycle.

The queue needed to support:

- **reserve** — claim work for processing.
- **complete** — mark successfully processed work as finished.
- **fail** — report an unsuccessful processing attempt.

The interviewer then extended the basic queue with production-oriented behavior:

- **Timeouts and Retries** — Reserved work could time out if processing did not complete within the expected window. Failed or expired work needed to support retry behavior.
- **Dead-Letter Queue (DLQ)** — Work that could no longer be successfully processed after the allowed retry behavior needed to be moved into a DLQ rather than continuously recycled through the main queue.

The round was therefore as much about **state transitions and failure handling** as about the core queue data structure.
