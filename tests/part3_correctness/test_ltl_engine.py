import unittest
import os
import sys
import yaml
import sqlite3
import logging

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from core.engine.clotho_simulator import Simulator

logger = logging.getLogger(__name__)

class TestLTL(unittest.TestCase):
    def setUp(self):
        self.test_file = os.path.join(os.path.dirname(__file__), 'ltl_scenario.yaml')
        with open(self.test_file, 'r') as f:
            self.clotho_data = yaml.safe_load(f)
        
        # Ensure clean state
        if os.path.exists("run_test_ltl.sqlite"):
            os.remove("run_test_ltl.sqlite")

    def tearDown(self):
        # Clean up
        for file in os.listdir('.'):
            if file.startswith('run_') and file.endswith('.sqlite'):
                try:
                    os.remove(file)
                except:
                    pass

    def test_ltl_checker(self):
        """Test that the simulator checks LTL invariants."""
        sim = Simulator(self.clotho_data)
        
        # Run simulation
        sim.run()
        
        # Check if invariants were verified
        if hasattr(sim, 'verify_invariants'):
            results = sim.verify_invariants()
            self.assertTrue(results['Liveness'], "Liveness property should hold")
        else:
            logger.warning("verify_invariants not implemented yet")
            # Fail the test if the feature is missing, as this is what we are developing
            self.fail("verify_invariants method is missing in Simulator")

        sim._close_db()

if __name__ == '__main__':
    unittest.main()
