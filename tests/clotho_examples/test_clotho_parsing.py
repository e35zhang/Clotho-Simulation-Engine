import unittest
import os
from core.engine.clotho_parser import load_clotho_from_file, ClothoValidationError

class TestClothoParsing(unittest.TestCase):
    def setUp(self):
        self.base_path = os.path.dirname(os.path.abspath(__file__))

    def test_valid_basic(self):
        file_path = os.path.join(self.base_path, 'valid_basic.yaml')
        data = load_clotho_from_file(file_path)
        self.assertIsNotNone(data)
        self.assertIn('types', data)
        self.assertIn('design', data)
        self.assertIn('run', data)
        
        # Check deep structure
        self.assertEqual(data['design']['components'][0]['name'], 'AccountService')
        self.assertEqual(len(data['run']['generators']), 1)

    def test_valid_complex_expressions(self):
        file_path = os.path.join(self.base_path, 'valid_complex_expressions.yaml')
        data = load_clotho_from_file(file_path)
        self.assertIsNotNone(data)
        
        # Verify invariants are parsed as strings (not evaluated yet)
        invariants = data['design']['components'][0]['invariants']
        self.assertTrue(any("all(read.RiskEngine.scores.score)" in inv for inv in invariants))

    def test_invalid_structure(self):
        file_path = os.path.join(self.base_path, 'invalid_structure.yaml')
        with self.assertRaises(ClothoValidationError) as cm:
            load_clotho_from_file(file_path)
        
        self.assertIn("missing required section", str(cm.exception))

    def test_types_and_constraints(self):
        """Verify Part 2: Types, Constraints, and Messages"""
        file_path = os.path.join(self.base_path, 'test_types_and_constraints.yaml')
        data = load_clotho_from_file(file_path)
        
        # Check Types
        types = data['types']
        self.assertIn('Money', types)
        self.assertEqual(types['Money']['constraints']['min'], 0)
        self.assertIn('AccountId', types)
        self.assertEqual(types['AccountId']['pattern'], "^ACC_[0-9]{4}$")
        
        # Check Messages
        messages = data['design']['messages']
        self.assertIn('InitiateTransfer', messages)
        self.assertEqual(messages['InitiateTransfer']['amount'], 'Money')
        self.assertEqual(messages['FraudCheckResult']['reason'], 'string?')

    def test_design_topology(self):
        """Verify Part 3: Design, Storage, and Logic"""
        file_path = os.path.join(self.base_path, 'test_design_topology.yaml')
        data = load_clotho_from_file(file_path)
        
        comp = data['design']['components'][0]
        self.assertEqual(comp['name'], 'AccountService')
        
        # Check Storage
        self.assertIn('state', comp)
        self.assertTrue(comp['state'][0]['schema']['id']['pk'])
        
        # Check Logic Structure
        handler = comp['handlers'][0]
        logic = handler['logic']
        self.assertEqual(len(logic), 3) # 2 reads + 1 match
        self.assertIn('read', logic[0])
        self.assertIn('match', logic[2])
        
        # Check Match Cases
        match_block = logic[2]['match']
        self.assertEqual(len(match_block['cases']), 2) # true case + default case

    def test_correctness_rules(self):
        """Verify Part 4: Invariants and Properties"""
        file_path = os.path.join(self.base_path, 'test_correctness_rules.yaml')
        data = load_clotho_from_file(file_path)
        
        # Check Invariants
        invariants = data['test']['invariants']
        self.assertEqual(len(invariants), 2)
        self.assertEqual(invariants[0]['severity'], 'CRITICAL')
        self.assertIn('sum(', invariants[1]['check'])
        
        # Check Properties (LTL)
        properties = data['test']['properties']
        self.assertEqual(len(properties), 2)
        self.assertIn('always(', properties[0]['assert'])
        self.assertIn('p99(', properties[1]['assert'])

    def test_run_evolution(self):
        """Verify Part 5: Run, Generators, and Faults"""
        file_path = os.path.join(self.base_path, 'test_run_evolution.yaml')
        data = load_clotho_from_file(file_path)
        
        run = data['run']
        
        # Check Fixtures
        self.assertEqual(len(run['fixtures']), 1)
        self.assertEqual(run['fixtures'][0]['rows'][0]['balance'], 1000)
        
        # Check Environment & Faults
        env = run['environment']
        self.assertEqual(env['scheduler']['interleaving_mode'], 'Aggressive')
        self.assertEqual(len(env['faults']), 2)
        self.assertEqual(env['faults'][0]['type'], 'MessageDrop')
        
        # Check Generators & Fuzz Hints
        gens = run['generators']
        self.assertEqual(len(gens), 2)
        self.assertEqual(gens[0]['behavior']['fuzz_hint']['amount'], 'high_range')

if __name__ == '__main__':
    unittest.main()
