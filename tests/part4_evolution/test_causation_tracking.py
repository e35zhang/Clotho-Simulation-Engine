"""
Unit tests for causation ID tracking and DAG features.

Tests cover:
- Event ID generation and uniqueness
- Causation ID propagation through message sends
- Database schema and storage
- Handler execution logging
- DAG construction from event log
- Event chain traversal
- Critical path analysis
"""

import unittest
import sqlite3
import os
import tempfile
import shutil
from datetime import datetime

from core.engine.clotho_simulator import Simulator
from core.analysis.trace_analyzer import TraceAnalyzer


class TestCausationTracking(unittest.TestCase):
    """Test causation ID tracking in the simulator"""
    
    def setUp(self):
        """Set up test environment with a simple Clotho blueprint"""
        self.test_dir = tempfile.mkdtemp()
        self.original_cwd = os.getcwd()
        os.chdir(self.test_dir)
        
        # Simple test blueprint with message passing
        self.test_Clotho = {
            'clotho_version': '3.0',
            'types': {},
            'design': {
                'components': [
                    {
                        'name': 'ServiceA',
                        'state': [
                            {
                                'name': 'data',
                                'schema': {
                                    'id': {'type': 'TEXT', 'pk': True},
                                    'value': {'type': 'INTEGER'}
                                }
                            }
                        ],
                        'handlers': [
                            {
                                'on_message': 'StartFlow',
                                'logic': [
                                    {
                                        'create': 'data',
                                        'data': {
                                            'id': '{{trigger.payload.id}}',
                                            'value': '{{trigger.payload.value}}'
                                        }
                                    },
                                    {
                                        'send': {
                                            'to': 'ServiceB',
                                            'message': 'ProcessData',
                                            'payload': {
                                                'id': '{{trigger.payload.id}}',
                                                'value': '{{trigger.payload.value}}'
                                            }
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'name': 'ServiceB',
                        'state': [
                            {
                                'name': 'processed',
                                'schema': {
                                    'id': {'type': 'TEXT', 'pk': True},
                                    'result': {'type': 'INTEGER'}
                                }
                            }
                        ],
                        'handlers': [
                            {
                            'on_message': 'ProcessData',
                            'logic': [
                                {
                                    'create': 'processed',
                                    'data': {
                                        'id': '{{trigger.payload.id}}',
                                        'result': '{{trigger.payload.value * 2}}'
                                    }
                                },
                                {
                                    'send': {
                                        'to': 'ServiceC',
                                        'message': 'FinalizeData',
                                        'payload': {
                                            'id': '{{trigger.payload.id}}',
                                            'result': '{{trigger.payload.value * 2}}'
                                        }
                                    }
                                }
                            ]
                        }
                    ]
                },
                {
                    'name': 'ServiceC',
                    'state': [
                        {
                            'name': 'final',
                            'schema': {
                                'id': {'type': 'TEXT', 'pk': True},
                                'final_value': {'type': 'INTEGER'}
                            }
                        }
                    ],
                    'handlers': [
                        {
                            'on_message': 'FinalizeData',
                            'logic': [
                                {
                                    'create': 'final',
                                    'data': {
                                        'id': '{{trigger.payload.id}}',
                                        'final_value': '{{trigger.payload.result}}'
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        },
            'test': {},
            'run': {
                'scenarios': [
                    {
                        'name': 'SimpleFlow',
                        'description': 'Test causation chain',
                        'initial_state': [
                            {'component': 'ServiceA', 'state': {'data': []}},
                            {'component': 'ServiceB', 'state': {'processed': []}},
                            {'component': 'ServiceC', 'state': {'final': []}}
                        ],
                        'steps': [
                            {
                                'send': 'StartFlow',
                                'to': 'ServiceA',
                                'from': '_External',
                                'payload': {'id': 'test_1', 'value': 10}
                            }
                        ]
                    }
                ]
            }
        }
    
    def tearDown(self):
        """Clean up test environment"""
        os.chdir(self.original_cwd)
        shutil.rmtree(self.test_dir)
    
    def test_event_id_generation(self):
        """Test that each handler execution gets a unique event_id"""
        simulator = Simulator(self.test_Clotho)
        simulator.select_scenario('SimpleFlow')
        
        # Run simulation
        simulator.run()
        
        # Check database
        conn = sqlite3.connect(simulator.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT DISTINCT event_id FROM event_log')
        event_ids = [row[0] for row in cursor.fetchall()]
        
        # Should have multiple unique event IDs
        self.assertGreater(len(event_ids), 0, "No event IDs generated")
        self.assertEqual(len(event_ids), len(set(event_ids)), "Event IDs are not unique")
        
        # Check format: evt_<12 hex chars>
        for event_id in event_ids:
            self.assertTrue(event_id.startswith('evt_'), f"Invalid event_id format: {event_id}")
            self.assertEqual(len(event_id), 16, f"Invalid event_id length: {event_id}")  # evt_ + 12 chars
        
        conn.close()
    
    def test_causation_id_propagation(self):
        """Test that causation_id is correctly propagated through message sends"""
        simulator = Simulator(self.test_Clotho)
        simulator.select_scenario('SimpleFlow')
        
        simulator.run()
        
        conn = sqlite3.connect(simulator.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get all handler executions (not individual writes)
        cursor.execute('''
            SELECT DISTINCT event_id, handler_name, causation_id 
            FROM event_log 
            WHERE action = 'HANDLER_EXEC'
            ORDER BY timestamp
        ''')
        events = cursor.fetchall()
        
        # Should have 3 handler executions: StartFlow -> ProcessData -> FinalizeData
        self.assertGreaterEqual(len(events), 3, "Not enough handler executions logged")
        
        # First event (StartFlow) should have no parent (root event)
        start_event = events[0]
        self.assertIsNone(start_event['causation_id'], "Root event should have no parent")
        self.assertIn('StartFlow', start_event['handler_name'])
        
        # Second event (ProcessData) should have StartFlow as parent
        process_event = events[1]
        self.assertIsNotNone(process_event['causation_id'], "Child event should have parent")
        self.assertEqual(process_event['causation_id'], start_event['event_id'], 
                        "ProcessData should have StartFlow as parent")
        
        # Third event (FinalizeData) should have ProcessData as parent
        finalize_event = events[2]
        self.assertIsNotNone(finalize_event['causation_id'], "Child event should have parent")
        self.assertEqual(finalize_event['causation_id'], process_event['event_id'],
                        "FinalizeData should have ProcessData as parent")
        
        conn.close()
    
    def test_handler_execution_logging(self):
        """Test that all handler executions are logged, even those without writes"""
        # Create blueprint with handler that only sends (no writes)
        Clotho_with_no_writes = self.test_Clotho.copy()
        Clotho_with_no_writes['design']['components'][0]['handlers'].append({
            'on_message': 'ForwardOnly',
            'logic': [
                {
                    'send': {
                        'to': 'ServiceB',
                        'message': 'ProcessData',
                        'payload': {'id': 'fwd_1', 'value': 5}
                    }
                }
            ]
        })
        Clotho_with_no_writes['run']['scenarios'][0]['steps'].append({
            'send': 'ForwardOnly',
            'to': 'ServiceA',
            'from': '_External',
            'payload': {}
        })
        
        simulator = Simulator(Clotho_with_no_writes)
        simulator.select_scenario('SimpleFlow')
        
        simulator.run()
        
        conn = sqlite3.connect(simulator.db_path)
        cursor = conn.cursor()
        
        # Check that ForwardOnly handler execution is logged
        cursor.execute('''
            SELECT COUNT(*) FROM event_log 
            WHERE handler_name LIKE '%ForwardOnly%' AND action = 'HANDLER_EXEC'
        ''')
        count = cursor.fetchone()[0]
        
        self.assertGreater(count, 0, "Handler with no writes should still be logged")
        
        conn.close()
    
    def test_correlation_id_consistency(self):
        """Test that all events in a scenario share the same correlation_id"""
        simulator = Simulator(self.test_Clotho)
        simulator.select_scenario('SimpleFlow')
        
        simulator.run()
        
        conn = sqlite3.connect(simulator.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT DISTINCT correlation_id FROM event_log')
        correlation_ids = [row[0] for row in cursor.fetchall()]
        
        # All events should share the same correlation_id
        self.assertEqual(len(correlation_ids), 1, 
                        f"Expected 1 correlation_id, found {len(correlation_ids)}")
        
        # Check format: tx_<8 hex chars>
        cid = correlation_ids[0]
        self.assertTrue(cid.startswith('tx_'), f"Invalid correlation_id format: {cid}")
        self.assertEqual(len(cid), 11, f"Invalid correlation_id length: {cid}")  # tx_ + 8 chars
        
        conn.close()
    
    def test_database_schema(self):
        """Test that event_log table has required causation columns"""
        simulator = Simulator(self.test_Clotho)
        simulator.select_scenario('SimpleFlow')
        
        simulator.run()
        
        conn = sqlite3.connect(simulator.db_path)
        cursor = conn.cursor()
        
        cursor.execute('PRAGMA table_info(event_log)')
        columns = {row[1]: row[2] for row in cursor.fetchall()}  # name: type
        
        # Check required columns exist
        required_columns = ['event_id', 'causation_id', 'handler_name', 'trigger_message']
        for col in required_columns:
            self.assertIn(col, columns, f"Missing required column: {col}")
        
        # Check data types
        self.assertEqual(columns['event_id'], 'TEXT', "event_id should be TEXT")
        self.assertEqual(columns['causation_id'], 'TEXT', "causation_id should be TEXT")
        
        conn.close()


class TestDAGAnalysis(unittest.TestCase):
    """Test DAG analysis methods in TraceAnalyzer"""
    
    def setUp(self):
        """Set up test database with known causation structure"""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, 'test.sqlite')
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create event_log table
        cursor.execute('''
            CREATE TABLE event_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT,
                timestamp TEXT,
                correlation_id TEXT,
                causation_id TEXT,
                component TEXT,
                handler_name TEXT,
                trigger_message TEXT,
                table_name TEXT,
                action TEXT,
                row_id TEXT,
                payload TEXT
            )
        ''')
        
        # Insert test events with known causation chain
        # Chain: A -> B -> C
        #              -> D
        events = [
            ('evt_a', '2025-01-01T10:00:00', 'tx_1', None, 'CompA', 'HandlerA', 'StartMsg', 'CompA_handler', 'HANDLER_EXEC', None, '{}'),
            ('evt_b', '2025-01-01T10:00:01', 'tx_1', 'evt_a', 'CompB', 'HandlerB', 'MsgB', 'CompB_handler', 'HANDLER_EXEC', None, '{}'),
            ('evt_c', '2025-01-01T10:00:02', 'tx_1', 'evt_b', 'CompC', 'HandlerC', 'MsgC', 'CompC_handler', 'HANDLER_EXEC', None, '{}'),
            ('evt_d', '2025-01-01T10:00:03', 'tx_1', 'evt_b', 'CompD', 'HandlerD', 'MsgD', 'CompD_handler', 'HANDLER_EXEC', None, '{}'),
        ]
        
        cursor.executemany('''
            INSERT INTO event_log (event_id, timestamp, correlation_id, causation_id, component, 
                                  handler_name, trigger_message, table_name, action, row_id, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', events)
        
        conn.commit()
        conn.close()
        
        self.analyzer = TraceAnalyzer(self.db_path)
    
    def tearDown(self):
        """Clean up test environment"""
        shutil.rmtree(self.test_dir)
    
    def test_get_trace_as_dag_structure(self):
        """Test that get_trace_as_dag returns correct structure"""
        dag = self.analyzer.get_trace_as_dag('tx_1')
        
        # Check structure
        self.assertIn('nodes', dag, "DAG should have 'nodes' key")
        self.assertIn('edges', dag, "DAG should have 'edges' key")
        
        # Check nodes
        self.assertEqual(len(dag['nodes']), 4, "Should have 4 nodes")
        
        node_ids = {node['event_id'] for node in dag['nodes']}
        self.assertEqual(node_ids, {'evt_a', 'evt_b', 'evt_c', 'evt_d'}, "Wrong node IDs")
        
        # Check edges (A->B, B->C, B->D)
        self.assertEqual(len(dag['edges']), 3, "Should have 3 edges")
        
        edges = {(e['from'], e['to']) for e in dag['edges']}
        expected_edges = {('evt_a', 'evt_b'), ('evt_b', 'evt_c'), ('evt_b', 'evt_d')}
        self.assertEqual(edges, expected_edges, "Wrong edges")
    
    def test_get_trace_as_dag_root_identification(self):
        """Test that root nodes (no parent) are correctly identified"""
        dag = self.analyzer.get_trace_as_dag('tx_1')
        
        # Find nodes with no incoming edges
        node_ids = {node['event_id'] for node in dag['nodes']}
        has_parent = {edge['to'] for edge in dag['edges']}
        roots = node_ids - has_parent
        
        self.assertEqual(roots, {'evt_a'}, "evt_a should be the only root")
    
    def test_get_event_chain_backward(self):
        """Test backward traversal (ancestors)"""
        chain = self.analyzer.get_event_chain('evt_c', direction='backward')
        
        # evt_c -> evt_b -> evt_a (backward traversal)
        # get_event_chain returns list of event_ids
        self.assertGreaterEqual(len(chain), 3, "Chain should include at least 3 events")
        
        self.assertIn('evt_c', chain)
        self.assertIn('evt_b', chain)
        self.assertIn('evt_a', chain)
    
    def test_get_event_chain_forward(self):
        """Test forward traversal (descendants)"""
        chain = self.analyzer.get_event_chain('evt_b', direction='forward')
        
        # evt_b -> evt_c, evt_d (forward traversal)
        # get_event_chain returns list of event_ids
        self.assertGreaterEqual(len(chain), 3, "Chain should include at least 3 events")
        
        self.assertIn('evt_b', chain)
        self.assertIn('evt_c', chain)
        self.assertIn('evt_d', chain)
    
    def test_get_critical_path(self):
        """Test critical path identification"""
        critical_path = self.analyzer.get_critical_path('tx_1')
        
        # Should return longest path: A -> B -> C (3 events)
        # get_critical_path returns list of event_ids
        self.assertGreaterEqual(len(critical_path), 3, "Critical path should have at least 3 events")
        
        # The path should be in order from root to leaf
        # For our test data: evt_a -> evt_b -> evt_c
        self.assertEqual(critical_path[0], 'evt_a', "Path should start with root")
        self.assertIn('evt_b', critical_path, "Path should include evt_b")
        self.assertIn('evt_c', critical_path, "Path should include evt_c")
    
    def test_dag_with_no_events(self):
        """Test DAG analysis with empty database"""
        empty_db = os.path.join(self.test_dir, 'empty.sqlite')
        conn = sqlite3.connect(empty_db)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE event_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT,
                causation_id TEXT,
                correlation_id TEXT,
                timestamp TEXT,
                component TEXT,
                handler_name TEXT,
                trigger_message TEXT,
                table_name TEXT,
                action TEXT,
                row_id TEXT,
                payload TEXT
            )
        ''')
        conn.commit()
        conn.close()
        
        analyzer = TraceAnalyzer(empty_db)
        dag = analyzer.get_trace_as_dag('nonexistent')
        
        self.assertEqual(len(dag['nodes']), 0, "Empty DB should return no nodes")
        self.assertEqual(len(dag['edges']), 0, "Empty DB should return no edges")


class TestCausationEdgeCases(unittest.TestCase):
    """Test edge cases and error handling"""
    
    def test_circular_dependency_prevention(self):
        """Test that simulator doesn't create circular causation"""
        # This should be prevented by the event queue mechanism
        # but we test to ensure no cycles in the DAG
        
        test_dir = tempfile.mkdtemp()
        db_path = os.path.join(test_dir, 'test.sqlite')
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE event_log (
                event_id TEXT,
                causation_id TEXT,
                correlation_id TEXT,
                timestamp TEXT,
                component TEXT,
                handler_name TEXT,
                trigger_message TEXT,
                table_name TEXT,
                action TEXT,
                row_id TEXT,
                payload TEXT
            )
        ''')
        
        # Try to insert circular reference
        cursor.execute('''
            INSERT INTO event_log (event_id, causation_id, correlation_id, timestamp, 
                                  component, handler_name, trigger_message, table_name, 
                                  action, row_id, payload)
            VALUES ('evt_a', 'evt_b', 'tx_1', '2025-01-01T10:00:00', 'CompA', 'HandlerA', 
                    'Msg', 'CompA_handler', 'HANDLER_EXEC', NULL, '{}'),
                   ('evt_b', 'evt_a', 'tx_1', '2025-01-01T10:00:01', 'CompB', 'HandlerB',
                    'Msg', 'CompB_handler', 'HANDLER_EXEC', NULL, '{}')
        ''')
        conn.commit()
        
        analyzer = TraceAnalyzer(db_path)
        
        # get_event_chain should handle cycles gracefully
        try:
            chain = analyzer.get_event_chain('evt_a', direction='backward')
            # Should not infinite loop
            self.assertLess(len(chain), 100, "Chain traversal should prevent infinite loops")
        finally:
            conn.close()
            shutil.rmtree(test_dir)
    
    def test_multiple_correlation_ids(self):
        """Test DAG analysis with multiple independent transactions"""
        test_dir = tempfile.mkdtemp()
        db_path = os.path.join(test_dir, 'test.sqlite')
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE event_log (
                event_id TEXT,
                causation_id TEXT,
                correlation_id TEXT,
                timestamp TEXT,
                component TEXT,
                handler_name TEXT,
                trigger_message TEXT,
                table_name TEXT,
                action TEXT,
                row_id TEXT,
                payload TEXT
            )
        ''')
        
        # Insert events for two separate transactions
        events = [
            ('evt_1a', None, 'tx_1', '2025-01-01T10:00:00', 'CompA', 'HandlerA', 'Msg', 'CompA_handler', 'HANDLER_EXEC', None, '{}'),
            ('evt_1b', 'evt_1a', 'tx_1', '2025-01-01T10:00:01', 'CompB', 'HandlerB', 'Msg', 'CompB_handler', 'HANDLER_EXEC', None, '{}'),
            ('evt_2a', None, 'tx_2', '2025-01-01T10:01:00', 'CompA', 'HandlerA', 'Msg', 'CompA_handler', 'HANDLER_EXEC', None, '{}'),
            ('evt_2b', 'evt_2a', 'tx_2', '2025-01-01T10:01:01', 'CompB', 'HandlerB', 'Msg', 'CompB_handler', 'HANDLER_EXEC', None, '{}'),
        ]
        
        cursor.executemany('''
            INSERT INTO event_log (event_id, causation_id, correlation_id, timestamp, component,
                                  handler_name, trigger_message, table_name, action, row_id, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', events)
        conn.commit()
        conn.close()
        
        analyzer = TraceAnalyzer(db_path)
        
        # Get DAG for tx_1 only
        dag1 = analyzer.get_trace_as_dag('tx_1')
        self.assertEqual(len(dag1['nodes']), 2, "tx_1 should have 2 nodes")
        
        # Get DAG for tx_2 only
        dag2 = analyzer.get_trace_as_dag('tx_2')
        self.assertEqual(len(dag2['nodes']), 2, "tx_2 should have 2 nodes")
        
        # Ensure no cross-contamination
        nodes1 = {n['event_id'] for n in dag1['nodes']}
        nodes2 = {n['event_id'] for n in dag2['nodes']}
        self.assertEqual(nodes1 & nodes2, set(), "DAGs should be independent")
        
        # Close analyzer connection before cleanup
        analyzer.close()
        
        # Force garbage collection to release file handles
        import gc
        gc.collect()
        
        # Retry deletion if file is locked
        import time
        for _ in range(3):
            try:
                shutil.rmtree(test_dir)
                break
            except PermissionError:
                time.sleep(0.1)


if __name__ == '__main__':
    unittest.main()
