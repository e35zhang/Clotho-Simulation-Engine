import unittest
import os
import json
from core.engine.clotho_simulator import Simulator
from core.engine.clotho_parser import load_clotho_from_file

class TestSimulatorIntegration(unittest.TestCase):
    def setUp(self):
        self.base_path = os.path.dirname(os.path.abspath(__file__))
        self.yaml_path = os.path.join(self.base_path, '..', 'clotho_examples', 'valid_basic.yaml')
        self.clotho_data = load_clotho_from_file(self.yaml_path)

    def test_run_basic_simulation(self):
        """
        Test that the simulator can:
        1. Load a Clotho YAML
        2. Apply compatibility layer
        3. Execute the synthesized scenario
        4. Produce event logs
        """
        sim = Simulator(self.clotho_data, simulation_seed=12345)
        # Select the auto-generated scenario
        sim.select_scenario('Auto-Generated Simulation')
        
        # Run simulation
        sim.run()
        
        # Verify execution
        # We expect at least:
        # 1. Initial state setup (fixtures - none in basic.yaml but logic creates account)
        # 2. Generator sending 'CreateAccount'
        # 3. Handler 'CreateAccount' executing
        # 4. 'create' action on 'accounts' table
        
        # Check DB for event log
        import sqlite3
        conn = sqlite3.connect(sim.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM event_log ORDER BY timestamp ASC")
        logs = [dict(row) for row in cursor.fetchall()]
        
        self.assertTrue(len(logs) > 0, "Event log should not be empty")
        
        # Find the handler execution
        handler_exec = next((l for l in logs if l['action'] == 'HANDLER_EXEC' and l['handler_name'] == 'CreateAccount'), None)
        self.assertIsNotNone(handler_exec, "Should have executed CreateAccount handler")
        
        # Find the CREATE action
        create_action = next((l for l in logs if l['action'] == 'CREATE' and l['table_name'] == 'AccountService_accounts'), None)
        self.assertIsNotNone(create_action, "Should have created an account record")
        
        payload = json.loads(create_action['payload'])
        self.assertEqual(payload['account_id'], 'acc_123')
        self.assertEqual(payload['balance'], 0)
        
        conn.close()

    def test_invariant_checking(self):
        """
        Test that invariants are checked during execution.
        We'll modify the YAML in memory to inject a failing invariant.
        """
        # Inject a failing invariant: balance must be > 100 (but we create with 0)
        self.clotho_data['design']['components'][0]['invariants'] = ["sum(read.AccountService.accounts.balance) > 100"]
        
        # Use a different seed to avoid file lock issues with previous test
        sim = Simulator(self.clotho_data, simulation_seed=12346)
        sim.select_scenario('Auto-Generated Simulation')
        
        # Expect RuntimeError due to invariant failure
        with self.assertRaises(RuntimeError) as cm:
            sim.run()
        
        self.assertIn("Invariant FAILED", str(cm.exception))
        
        import sqlite3
        conn = sqlite3.connect(sim.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM event_log WHERE action = 'INVARIANT_FAIL'")
        failures = cursor.fetchall()
        
        self.assertTrue(len(failures) > 0, "Should have logged invariant failure")
        conn.close()
        
        self.assertTrue(len(failures) > 0, "Should have detected invariant failure")
        conn.close()
        payload = json.loads(failures[0]['payload'])
        self.assertIn("sum(read.AccountService.accounts.balance) > 100", payload['invariant'])

if __name__ == '__main__':
    unittest.main()
