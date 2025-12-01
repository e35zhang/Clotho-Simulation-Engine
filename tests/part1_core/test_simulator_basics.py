# tests/test_simulator_basics.py
"""
Simulator Core Basics Tests

Tests fundamental simulator initialization, scenario selection, and database schema creation.
These are the foundational tests that verify the simulator can bootstrap properly.

Coverage:
- Simulator initialization
- Database schema creation from blueprint
- Scenario selection and validation
"""

import unittest
import os
from core.engine.clotho_simulator import Simulator


class TestSimulatorCoreBasics(unittest.TestCase):
    """Test basic simulator initialization and setup"""

    def test_simulator_initialization(self):
        """Test that simulator initializes correctly."""
        test_blueprint = {
            'clotho_version': '1.0',
            'types': {},
            'design': {'components': []},
            'test': {},
            'run': {'scenarios': [{'name': 'test', 'initial_state': [], 'steps': []}]}
        }
        
        sim = Simulator(test_blueprint)
        self.assertIsNotNone(sim)
        self.assertEqual(sim.clotho_data, test_blueprint)

    def test_database_schema_creation(self):
        """Test that database schema is created correctly from blueprint."""
        test_blueprint = {
            'clotho_version': '1.0',
            'types': {},
            'design': {
                'components': [
                    {
                        'name': 'TestComponent',
                        'state': [
                            {
                                'name': 'users',
                                'schema': {
                                    'id': {'type': 'INTEGER', 'pk': True},
                                    'name': {'type': 'TEXT'}
                                }
                            }
                        ],
                        'handlers': []
                    }
                ]
            },
            'test': {},
            'run': {
                'scenarios': [{'name': 'test_scenario', 'initial_state': [], 'steps': []}]
            }
        }

        sim = Simulator(test_blueprint)
        sim.select_scenario('test_scenario')
        
        # Run simulation to create schema
        sim.run()
        
        # Verify table was created - reconnect to closed database
        import sqlite3
        conn = sqlite3.connect(sim.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='TestComponent_users'")
        result = cursor.fetchone()
        self.assertIsNotNone(result)
        conn.close()

    def test_scenario_selection(self):
        """Test scenario selection."""
        test_blueprint = {
            'clotho_version': '1.0',
            'types': {},
            'design': {'components': []},
            'test': {},
            'run': {
                'scenarios': [
                    {'name': 'scenario_1', 'initial_state': [], 'steps': []},
                    {'name': 'scenario_2', 'initial_state': [], 'steps': []}
                ]
            }
        }
        
        sim = Simulator(test_blueprint)
        
        # Select first scenario
        sim.select_scenario('scenario_1')
        self.assertEqual(sim.current_scenario['name'], 'scenario_1')
        
        # Select second scenario
        sim.select_scenario('scenario_2')
        self.assertEqual(sim.current_scenario['name'], 'scenario_2')


if __name__ == '__main__':
    unittest.main()
