##What problem does this work address?

Stream processing engines often disagree on how window operators behave when data arrives out of order or late. This creates portability and correctness issues: the same window specification can include/exclude different events or emit different results depending on the engine’s notion of progress (e.g., watermarks or stream time) and its reporting style.
The paper clarifies this by presenting a unified lifecycle for window operators—covering event inclusion, triggering, and reporting—so developers can reason about behaviour consistently across engines.

##Which systems were evaluated?

The study compares representative, widely used engines:

- Apache Beam — model-first, with configurable triggers and accumulation modes.

- Apache Flink — watermark-driven event time with allowedLateness and custom triggers.

- Spark Structured Streaming — watermark-based state management and output modes.

- Kafka Streams — stream-time progress with grace (and optional suppress on close).

- Apache Storm — single-buffer windowing with trigger/evict policies and lag-based lateness.

Window types examined include sliding, tumbling, and session windows, focusing on how each engine accepts events into windows and reports results under late and out-of-order arrivals.
