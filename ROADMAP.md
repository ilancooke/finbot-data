# Roadmap

This project is intentionally starting small: containerized scripts that download, normalize, and store market data. These are the likely next steps as the data store grows.

## Future Data Jobs

- Add new Sharadar/Nasdaq Data Link datasets only when they have a clear downstream consumer.
- Keep each data pull as a focused script with shared helpers for table export handling, storage, metadata, and ticker universe loading.

## Orchestration

- Possibly add an orchestrator so one containerized job can run after another.
- Avoid turning every data pull into one giant script; prefer small jobs that can be scheduled and debugged independently.

## Operations

- Add stronger operational checks around logging, metadata, retry behavior, partial failure reporting, and alerts.
- Make failures easy to inspect after an overnight scheduled run.
