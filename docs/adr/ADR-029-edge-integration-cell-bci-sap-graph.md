# ADR-029: Hospital Edge DSP & SAP Graph Semantic Federation for BCI Clinical Trials

## Status
Accepted & Deployed

## Context
High-frequency intracranial Brain-Computer Interface (BCI) implants generate terabytes of raw EEG/ECoG microvolt telemetry daily. Transmitting uncleaned waveforms containing patient identifiers directly to cloud data lakes violates HIPAA/GDPR residency constraints and creates unacceptable WAN latency for seizure precursor detection.

## Decision
1. **On-Premise Edge DSP via SAP Edge Integration Cell:** Deploy Groovy-based Exponential Moving Average (EMA) low-pass filtering and RMS spectral amplitude calculation inside the hospital Edge node to denoise and compress neural telemetry in sub-millisecond latency.
2. **Zero-Knowledge Edge Pseudonymization:** Strip patient Medical Record Numbers (MRNs) and generate salted SHA-256 subject tokens before any packet leaves the hospital firewall.
3. **Parallel Multicast to Databricks & S/4HANA:** Stream compressed neural feature vectors to Databricks Delta Lake / Dataiku ML while routing critical pre-ictal alerts to SAP S/4HANA Life Sciences.
4. **Zero-ETL Federation via SAP Graph:** Expose a unified GraphQL schema joining S/4HANA clinical trial subject records with Databricks neural analytics without physical database duplication.
