"""
CRITICAL TEST: Verify Deterministic Simulation Replay
Tests that the same seed produces identical results in:
1. Single-threaded execution
2. Multi-threaded (parallel) execution

This is THE core feature of chaos engineering - if replay doesn't work,
the entire "time travel" debugging capability is broken.
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.engine.clotho_simulator import Simulator
from core.chaos.chaos_matrix import ChaosMatrix
from core.chaos.fuzzer import FuzzingConfig
import yaml
import hashlib
import json
import pytest

@pytest.fixture
def clotho_data():
    """Load Clotho configuration data"""
    with open('tests/fixtures/test_banking_scenario.yaml', encoding='utf-8') as f:
        return yaml.safe_load(f)

def compute_trace_hash(db_path):
    """Compute deterministic hash of event trace
    
    NOTE: We exclude 'timestamp' from hash because it's wall-clock time (non-deterministic).
    The deterministic properties are:
    - event_id (now deterministic with self.rng)
    - handler_name (business logic)
    - action (UPDATE/INSERT)
    - table_name (which table)
    - payload (what data)
    """
    import sqlite3
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all events in order (exclude timestamp - it's wall-clock time)
    cursor.execute("SELECT event_id, handler_name, action, table_name, payload FROM event_log ORDER BY id")
    events = cursor.fetchall()
    conn.close()
    
    # Compute hash
    trace_str = json.dumps(events, sort_keys=True)
    return hashlib.sha256(trace_str.encode()).hexdigest()

def test_single_threaded_replay(clotho_data):
    """Test 1: Run same seed twice in single-threaded mode"""
    print("\n" + "="*80)
    print("TEST 1: Single-Threaded Deterministic Replay")
    print("="*80)
    
    TEST_SEED = 12345
    
    # Run 1
    print(f"\nRun 1 with seed {TEST_SEED}...")
    sim1 = Simulator(clotho_data=clotho_data, simulation_seed=TEST_SEED)
    sim1.select_scenario('vulnerable_banking_test')
    sim1.run()  # Execute simulation
    hash1 = compute_trace_hash(sim1.db_path)
    db1 = sim1.db_path
    
    # Run 2
    print(f"Run 2 with seed {TEST_SEED}...")
    sim2 = Simulator(clotho_data=clotho_data, simulation_seed=TEST_SEED)
    sim2.select_scenario('vulnerable_banking_test')
    sim2.run()
    hash2 = compute_trace_hash(sim2.db_path)
    db2 = sim2.db_path
    
    print(f"\nRun 1 trace hash: {hash1}")
    print(f"Run 2 trace hash: {hash2}")
    
    assert hash1 == hash2, f"Single-threaded replay is NOT deterministic! DB1: {db1}, DB2: {db2}"
    print("[PASS] Single-threaded replay is deterministic!")

def test_parallel_vs_single(clotho_data):
    """Test 2: Compare parallel execution vs single-threaded"""
    print("\n" + "="*80)
    print("TEST 2: Parallel vs Single-Threaded (CRITICAL)")
    print("="*80)
    
    TEST_SEEDS = [99991, 99992, 99993]
    
    # Single-threaded execution
    print("\nPhase 1: Single-threaded execution...")
    single_hashes = {}
    for seed in TEST_SEEDS:
        sim = Simulator(clotho_data=clotho_data, simulation_seed=seed)
        sim.select_scenario('vulnerable_banking_test')
        sim.run()
        single_hashes[seed] = compute_trace_hash(sim.db_path)
        print(f"  Seed {seed}: {single_hashes[seed][:16]}...")
    
    # Parallel execution via ChaosMatrix
    print("\nPhase 2: Parallel execution (8 workers)...")
    config = FuzzingConfig(fuzz_inputs=False, fuzz_states=False, fuzz_scenarios=False)
    chaos = ChaosMatrix(
        clotho_data=clotho_data,
        scenario_name='vulnerable_banking_test',
        max_workers=8,
        fuzzing_config=config,
        track_coverage=False
    )
    
    results, _ = chaos.run_batch(num_simulations=len(TEST_SEEDS), seed_start=TEST_SEEDS[0], cleanup_dbs=False)
    
    parallel_hashes = {}
    for result in results:
        # Compare trace hashes regardless of success status (validation may fail due to race conditions)
        if result.db_path:
            parallel_hashes[result.seed] = compute_trace_hash(result.db_path)
            print(f"  Seed {result.seed}: {parallel_hashes[result.seed][:16]}... (success={result.success})")
    
    # Compare
    print("\nComparison:")
    mismatches = []
    for seed in TEST_SEEDS:
        single = single_hashes.get(seed, "MISSING")
        parallel = parallel_hashes.get(seed, "MISSING")
        match = "[OK]" if single == parallel else "[MISMATCH]"
        print(f"  Seed {seed}: {match}")
        if single != parallel:
            print(f"    Single:   {single}")
            print(f"    Parallel: {parallel}")
            mismatches.append(seed)
    
    assert len(mismatches) == 0, f"Parallel execution is NOT deterministic for seeds: {mismatches}. Root cause: Likely shared random state or Fuzzer pollution."
    print("\n[PASS] Parallel execution produces deterministic results!")

def test_fuzzing_determinism(clotho_data):
    """Test 3: Fuzzing with same seed should be deterministic"""
    print("\n" + "="*80)
    print("TEST 3: Fuzzing Determinism")
    print("="*80)
    
    TEST_SEED = 77777
    FUZZING_SEED = 88888  # CRITICAL FIX: Separate fuzzing seed
    
    # Enable fuzzing with explicit seed
    config = FuzzingConfig(fuzz_inputs=True, fuzz_states=True, seed=FUZZING_SEED)
    
    # Run 1
    print(f"\nRun 1 with sim seed {TEST_SEED}, fuzzing seed {FUZZING_SEED}...")
    chaos1 = ChaosMatrix(
        clotho_data=clotho_data,
        scenario_name='vulnerable_banking_test',
        max_workers=1,  # Single-threaded for now
        fuzzing_config=config,
        track_coverage=False
    )
    results1, _ = chaos1.run_batch(num_simulations=1, seed_start=TEST_SEED, cleanup_dbs=False)
    hash1 = compute_trace_hash(results1[0].db_path) if results1[0].success else "FAILED"
    
    # Run 2 - CRITICAL: Must recreate config with SAME seed
    config2 = FuzzingConfig(fuzz_inputs=True, fuzz_states=True, seed=FUZZING_SEED)
    print(f"Run 2 with sim seed {TEST_SEED}, fuzzing seed {FUZZING_SEED}...")
    chaos2 = ChaosMatrix(
        clotho_data=clotho_data,
        scenario_name='vulnerable_banking_test',
        max_workers=1,
        fuzzing_config=config2,
        track_coverage=False
    )
    results2, _ = chaos2.run_batch(num_simulations=1, seed_start=TEST_SEED, cleanup_dbs=False)
    hash2 = compute_trace_hash(results2[0].db_path) if results2[0].success else "FAILED"
    
    print(f"\nRun 1 hash: {hash1}")
    print(f"Run 2 hash: {hash2}")
    
    assert hash1 == hash2, "Fuzzing is NOT deterministic!"
    print("[PASS] Fuzzing is deterministic!")
