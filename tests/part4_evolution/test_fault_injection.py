import unittest
import os
import yaml
import logging
from core.engine.clotho_simulator import Simulator
from core.engine.clotho_parser import load_clotho_from_file

logger = logging.getLogger(__name__)

class TestFaultInjection(unittest.TestCase):
    def setUp(self):
        self.base_path = os.path.dirname(os.path.abspath(__file__))
        self.yaml_path = os.path.join(self.base_path, 'fault_injection_scenario.yaml')
        self.clotho_data = load_clotho_from_file(self.yaml_path)

    def test_message_drop_fault(self):
        """Test that MessageDrop fault actually drops messages."""
        sim = Simulator(self.clotho_data, simulation_seed=12345)
        sim.run()
        
        # Reconnect to DB to read results
        sim._connect_db(reset=False)
        
        # Analyze results
        sim.cursor.execute("SELECT COUNT(*) FROM event_log WHERE action='HANDLER_EXEC' AND handler_name='Ping'")
        sent_count = sim.cursor.fetchone()[0]
        
        sim.cursor.execute("SELECT COUNT(*) FROM event_log WHERE action='HANDLER_EXEC' AND handler_name='Pong'")
        received_count = sim.cursor.fetchone()[0]
        
        # Check for fault logs
        sim.cursor.execute("SELECT COUNT(*) FROM event_log WHERE table_name='FAULT' AND action='FAULT_INJECTION'")
        fault_count = sim.cursor.fetchone()[0]
        
        logger.info(f"Sent: {sent_count}, Received: {received_count}, Faults Injected: {fault_count}")
        
        # With 50% drop rate and 10 pings:
        # We expect roughly 5 pongs.
        # And roughly 5 faults.
        
        self.assertEqual(sent_count, 10, "Should have sent 10 Pings")
        self.assertLess(received_count, 10, "Should have dropped some Pongs")
        self.assertGreater(fault_count, 0, "Should have injected faults")
        self.assertEqual(sent_count, received_count + fault_count, "Sent should equal Received + Dropped")

if __name__ == '__main__':
    unittest.main()
