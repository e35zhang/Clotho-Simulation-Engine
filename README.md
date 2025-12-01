# Clotho-Engine

**HFT-Grade Deterministic Simulation & Fuzzing Engine**

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python: 3.10+](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Tests](https://img.shields.io/badge/Tests-165%20passed-brightgreen.svg)]()
[![Coverage](https://img.shields.io/badge/Coverage-63%25-yellow.svg)]()

---

## ❓ Why Clotho? (vs. Jepsen / Maelstrom)

While tools like **Jepsen** test distributed systems from the outside (black-box testing of deployed binaries), **Clotho** tests system logic from the inside (white-box deterministic simulation).

| Aspect | Jepsen | Clotho |
|--------|--------|--------|
| **Approach** | Black-box (test deployed binaries) | White-box (test logic directly) |
| **Speed** | Minutes (spins up containers/VMs) | **<10 seconds** (in-memory simulation) |
| **Debugging** | Log analysis after failure | **Time-travel debugging** (step through race conditions line-by-line) |
| **Setup** | Complex (Docker, network partitions) | Simple (`pip install` + YAML) |

**Inspiration:** Clotho brings the [FoundationDB simulation testing methodology](https://apple.github.io/foundationdb/testing.html) — testing the logic, not the network stack — to Python developers.

---

## 🚀 Getting Started

### Installation

```bash
pip install -r requirements.txt
```

### The Golden Demo

Run the banking race-condition demo to see Clotho catch bugs that traditional testing misses:

```bash
python examples/run_banking_demo.py
```

**Expected Output:**
```
🚨 Race Condition Detected! (Reliability Score: 45/100)
```

### Docker (Optional)

```bash
docker build -t clotho . && docker run --rm clotho
```

---

## 📖 Technical Architecture

### 1. Determinism & Replay

**Problem:** Distributed race conditions are non-reproducible—the same test passes 99 times, fails once.

**Solution:** `clotho_simulator.py` uses a **private RNG instance** seeded at initialization:

```python
self.simulation_seed = simulation_seed if simulation_seed is not None else random.randint(0, 2**32 - 1)
self.rng = random.Random(self.simulation_seed)  # Thread-safe private RNG
```

- **Bit-perfect replay:** Same seed → identical event ordering → identical race condition
- **Thread isolation:** Each simulation has its own RNG, preventing cross-contamination
- **Deterministic IDs:** Database paths and event IDs are generated from `self.rng`, not `uuid.uuid4()`

### 2. Statistical Fuzzing

**Problem:** Edge cases (integer overflow, null injection) are rarely hit by hand-written tests.

**Solution:** `fuzzer.py` implements three fuzzing strategies:

| Strategy | Description | Example |
|----------|-------------|---------|
| **Input Fuzzing** | Mutate message payloads | `amount: 100` → `amount: -1` or `amount: 2147483647` |
| **State Fuzzing** | Randomize initial DB states | Empty tables, extreme balances |
| **Scenario Fuzzing** | Interleave/shuffle event sequences | Test race conditions between transactions |

**Configuration:**
```python
FuzzingConfig(
    boundary_value_prob=0.3,   # 0, -1, MAX_INT
    type_confusion_prob=0.2,   # String ↔ Number
    null_prob=0.1,             # None injection
    extreme_value_prob=0.2     # Very large/small values
)
```

### 3. Formal Verification

**Problem:** "It works" ≠ "It's correct." How do you prove invariants hold across all executions?

**Solution:** `reliability_scorer.py` computes a **Reliability Score (0-100)** using:

- **Good-Turing estimation:** Predict unseen states from observed state frequencies
- **Wilson score interval:** Compute confidence bounds accounting for sample size
- **Coverage rate:** Unique states discovered ÷ total observations

**Score Interpretation:**
| Score | Grade | Meaning |
|-------|-------|---------|
| 95-100 | A | High confidence in correctness |
| 85-94 | B | Reasonable confidence |
| 70-84 | C | More testing recommended |
| < 70 | F | Insufficient coverage |

### 4. Causality Analysis

**Problem:** When a bug occurs, which event caused it? In distributed systems, causation ≠ correlation.

**Solution:** `trace_analyzer.py` builds a **DAG (Directed Acyclic Graph)** of events:

```python
dag = analyzer.get_trace_as_dag(correlation_id)
# Returns: {'nodes': [...], 'edges': [{'from': evt_1, 'to': evt_2}, ...]}

critical_path = analyzer.get_critical_path(correlation_id)
# Returns: ['evt_001', 'evt_003', 'evt_007']  # Longest causal chain
```

- **Correlation ID:** Groups all events in a transaction
- **Causation ID:** Links each event to its direct parent
- **Critical Path:** Finds the longest chain for performance analysis
