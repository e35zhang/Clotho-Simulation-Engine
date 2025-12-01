import unittest
import os
import logging
from core.engine.clotho_simulator import Simulator
from core.engine.clotho_parser import load_clotho_from_file

logger = logging.getLogger(__name__)

class TestRaceCondition(unittest.TestCase):
    def setUp(self):
        self.base_path = os.path.dirname(os.path.abspath(__file__))
        self.yaml_path = os.path.join(self.base_path, 'race_condition_scenario.yaml')
        self.clotho_data = load_clotho_from_file(self.yaml_path)

    def test_race_condition_detection(self):
        """
        Test that Aggressive scheduling exposes the race condition.
        We run the simulation multiple times with different seeds to find a schedule
        that triggers the race condition (Lost Update).
        """
        # Force Aggressive mode
        self.clotho_data['run']['environment']['scheduler']['interleaving_mode'] = 'Aggressive'
        
        found_race_condition = False
        
        # Try up to 20 seeds
        for seed in range(20):
            logger.info(f"--- Running with seed {seed} ---")
            sim = Simulator(self.clotho_data, simulation_seed=seed)
            sim.select_scenario('Auto-Generated Simulation')
            sim.run()
            
            # Check final value
            import sqlite3
            conn = sqlite3.connect(sim.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT value FROM CounterService_counters WHERE id = 'c1'")
            result = cursor.fetchone()
            final_value = result[0] if result else 0
            conn.close()
            
            logger.info(f"Seed {seed} -> Final Counter Value: {final_value}")
            
            if final_value == 1:
                found_race_condition = True
                logger.info("Race condition detected! (Lost Update)")
                break
        
        self.assertTrue(found_race_condition, "Should have detected race condition (value=1) in at least one run")

if __name__ == '__main__':
    unittest.main()
