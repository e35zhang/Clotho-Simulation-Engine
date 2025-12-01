"""
Quick parallel determinism test - only print final results
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Redirect verbose output to null
os.environ['PYTHONWARNINGS'] = 'ignore'

from core.chaos.chaos_matrix import ChaosMatrix
import yaml
import sqlite3
import pytest

@pytest.fixture
def clotho_data():
    """Load Clotho configuration data"""
    with open('tests/fixtures/test_banking_scenario.yaml', encoding='utf-8') as f:
        return yaml.safe_load(f)

def test_quick_parallel_determinism(clotho_data):
    """Quick test for parallel determinism with 4 workers"""
    seed = 12345    # Run 1
    matrix1 = ChaosMatrix(clotho_data=clotho_data, scenario_name='vulnerable_banking_test', max_workers=4)
    results1, _ = matrix1.run_batch(num_simulations=1, seed_start=seed, cleanup_dbs=False)
    result1 = results1[0]

    # Run 2
    matrix2 = ChaosMatrix(clotho_data=clotho_data, scenario_name='vulnerable_banking_test', max_workers=4)
    results2, _ = matrix2.run_batch(num_simulations=1, seed_start=seed, cleanup_dbs=False)
    result2 = results2[0]

    # Compare
    conn1 = sqlite3.connect(result1.db_path)
    conn1.row_factory = sqlite3.Row
    cursor1 = conn1.cursor()
    cursor1.execute("SELECT * FROM BankingService_accounts ORDER BY account_id")
    state1 = [dict(row) for row in cursor1.fetchall()]
    conn1.close()

    conn2 = sqlite3.connect(result2.db_path)
    conn2.row_factory = sqlite3.Row
    cursor2 = conn2.cursor()
    cursor2.execute("SELECT * FROM BankingService_accounts ORDER BY account_id")
    state2 = [dict(row) for row in cursor2.fetchall()]
    conn2.close()

    assert state1 == state2, f"NOT DETERMINISTIC! Run 1: {state1}, Run 2: {state2}"
    print(f"[OK] PARALLEL DETERMINISTIC with seed {seed}")
    print(f"  Alice: {state1[0]['balance']}, Bob: {state1[1]['balance']}, Charlie: {state1[2]['balance']}")
    print(f"  Run 2: {state2}")
