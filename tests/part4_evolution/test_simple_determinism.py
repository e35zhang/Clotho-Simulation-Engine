"""
Simple determinism test without fuzzing
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.engine.clotho_simulator import Simulator
import yaml
import sqlite3
import pytest

@pytest.fixture
def clotho_data():
    """Load Clotho configuration data"""
    with open('tests/fixtures/test_banking_scenario.yaml', encoding='utf-8') as f:
        return yaml.safe_load(f)

def test_simple_determinism(clotho_data):
    """Test that same seed produces same final state"""
    SEED = 12345

    # Run 1
    print(f"Run 1 with seed {SEED}...")
    sim1 = Simulator(clotho_data=clotho_data, simulation_seed=SEED)
    sim1.select_scenario('vulnerable_banking_test')
    sim1.run()

    conn1 = sqlite3.connect(sim1.db_path)
    conn1.row_factory = sqlite3.Row
    cursor1 = conn1.cursor()
    cursor1.execute("SELECT * FROM BankingService_accounts ORDER BY account_id")
    final_state1 = [dict(row) for row in cursor1.fetchall()]
    conn1.close()

    # Run 2
    print(f"Run 2 with seed {SEED}...")
    sim2 = Simulator(clotho_data=clotho_data, simulation_seed=SEED)
    sim2.select_scenario('vulnerable_banking_test')
    sim2.run()

    conn2 = sqlite3.connect(sim2.db_path)
    conn2.row_factory = sqlite3.Row
    cursor2 = conn2.cursor()
    cursor2.execute("SELECT * FROM BankingService_accounts ORDER BY account_id")
    final_state2 = [dict(row) for row in cursor2.fetchall()]
    conn2.close()

    print(f"\nRun 1 final state: {final_state1}")
    print(f"Run 2 final state: {final_state2}")

    assert final_state1 == final_state2, "Final states don't match - simulation is NOT deterministic!"
    print("\n[OK] DETERMINISTIC! Same seed produces same final state.")
