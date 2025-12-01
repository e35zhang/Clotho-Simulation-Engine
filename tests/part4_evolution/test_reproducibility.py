import unittest
import os
import yaml
import hashlib
import json
import logging
from core.engine.clotho_simulator import Simulator
from core.engine.clotho_parser import load_clotho_from_file

logger = logging.getLogger(__name__)

class TestReproducibility(unittest.TestCase):
    def setUp(self):
        self.base_path = os.path.dirname(os.path.abspath(__file__))
        # Use the race condition scenario as it has complex interleaving
        self.yaml_path = os.path.join(self.base_path, 'race_condition_scenario.yaml')
        self.clotho_data = load_clotho_from_file(self.yaml_path)
        
        # Force Aggressive mode to ensure we are testing the complex scheduler
        self.clotho_data['run']['environment']['scheduler']['interleaving_mode'] = 'Aggressive'

    def compute_trace_hash(self, db_path):
        """Compute deterministic hash of event trace"""
        import sqlite3
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all events in order (exclude timestamp - it's wall-clock time)
        # Also exclude event_id if it's not deterministic (but it SHOULD be)
        # We include event_id to verify our ID generation is also deterministic
        cursor.execute("SELECT event_id, handler_name, action, table_name, payload, correlation_id, causation_id FROM event_log ORDER BY id")
        events = cursor.fetchall()
        conn.close()
        
        # Compute hash
        # Convert rows to list of tuples for consistent serialization
        events_list = [tuple(row) for row in events]
        trace_str = json.dumps(events_list, sort_keys=True, default=str)
        return hashlib.sha256(trace_str.encode()).hexdigest()

    def test_deterministic_replay(self):
        """Test that running the same seed twice produces EXACTLY the same event log."""
        seed_a = 1
        
        # Run 1
        sim1 = Simulator(self.clotho_data, simulation_seed=seed_a)
        sim1.run()
        hash1 = self.compute_trace_hash(sim1.db_path)
        
        # Run 2 (Same Seed)
        sim2 = Simulator(self.clotho_data, simulation_seed=seed_a)
        sim2.run()
        hash2 = self.compute_trace_hash(sim2.db_path)
        
        logger.info(f"Run 1 Hash: {hash1}")
        logger.info(f"Run 2 Hash: {hash2}")
        
        self.assertEqual(hash1, hash2, "Event logs should be identical for same seed")

    def test_different_seeds_produce_different_traces(self):
        """Verify that different seeds actually produce different traces (sanity check)."""
        seed_a = 1
        seed_b = 2
        
        # Run A
        logger.info("--- Run A (Seed 1) ---")
        sim1 = Simulator(self.clotho_data, simulation_seed=seed_a)
        sim1.run()
        hash1 = self.compute_trace_hash(sim1.db_path)
        
        # Run B
        logger.info("--- Run 1 (Seed 2) ---")
        sim2 = Simulator(self.clotho_data, simulation_seed=seed_b)
        sim2.run()
        hash2 = self.compute_trace_hash(sim2.db_path)
        
        self.assertNotEqual(hash1, hash2, "Different seeds should produce different traces")

if __name__ == '__main__':
    unittest.main()
