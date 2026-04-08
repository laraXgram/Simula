# Debug Console Guide

The debug console is available at /debug in the client app.

## What it shows

- Realtime request/response runtime logs
- Structured webhook dispatch records
- Realtime websocket update stream
- JSON inspector with request/response/diff tabs

## Filters and Views

- Runtime logs: search, status filter, source filter
- Webhook viewer: status filter and webhook URL filter
- Updates stream: keyword search and expandable payloads

## JSON Diff Inspector

Diff mode compares flattened JSON paths from request and response and marks:

- added fields
- removed fields
- changed fields

## Trace Export

Use Export trace to generate a JSON bundle suitable for bug reports.

Bundle contains:

- runtime logs
- websocket updates
- selected bot token
- API base URL
- exported timestamp

## Trace Import

Use Import trace to load a previously exported bundle and inspect it offline.

When imported mode is active:

- runtime logs and updates are read from the trace file
- websocket live stream is disconnected
- inspector and filtering continue to work
