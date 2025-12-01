import sys
import os
import unittest
import time
import yaml
from functools import lru_cache

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.engine.expression_engine import evaluate, ExpressionInterpreter, cached_parse
from core.engine.static_analyzer import ClothoStaticAnalyzer
from core.engine.clotho_simulator import Simulator

class TestClothoFixes(unittest.TestCase):

    def test_expression_caching(self):
        print("\n--- Testing Expression Caching ---")
        expr = "a + b > 10"
        context = {'a': 5, 'b': 6}
        
        start = time.time()
        for _ in range(1000):
            evaluate(expr, context)
        end = time.time()
        print(f"1000 evaluations took: {end - start:.4f}s")
        
        # Verify cache info if available
        if hasattr(cached_parse, 'cache_info'):
            print(f"Cache Info: {cached_parse.cache_info()}")
            self.assertGreater(cached_parse.cache_info().hits, 0)

    def test_null_safety(self):
        print("\n--- Testing Null Safety ---")
        interpreter = ExpressionInterpreter({'a': 10, 'b': None})
        
        # Addition with None
        res = evaluate("a + b", interpreter)
        print(f"10 + None = {res}")
        self.assertIsNone(res)
        
        # Multiplication with None
        res = evaluate("a * b", interpreter)
        print(f"10 * None = {res}")
        self.assertIsNone(res)
        
        # Comparison with None
        res = evaluate("b > 5", interpreter)
        print(f"None > 5 = {res}")
        self.assertFalse(res) # Should be False, not crash
        
        res = evaluate("b == null", interpreter)
        print(f"None == null = {res}")
        self.assertTrue(res)

    def test_static_analysis(self):
        print("\n--- Testing Static Analysis ---")
        valid_yaml = {
            'design': {
                'components': [
                    {
                        'name': 'Account',
                        'state': [{'name': 'accounts', 'schema': {'balance': 'int'}}],
                        'handlers': [
                            {
                                'name': 'Deposit',
                                'on': 'DepositMsg',
                                'logic': [
                                    {'read': 'accounts', 'as': 'acc', 'key': '1'},
                                    {'create': 'accounts', 'data': {'balance': '{{ read.acc.balance + 100 }}'}}
                                ]
                            }
                        ]
                    }
                ]
            },
            'types': {'messages': {'DepositMsg': {'amount': 'int'}}}
        }
        
        analyzer = ClothoStaticAnalyzer(valid_yaml)
        errors = analyzer.analyze()
        self.assertIsNone(errors) # Should return None on success
        print("Valid YAML passed static analysis.")

        invalid_yaml = {
            'design': {
                'components': [
                    {
                        'name': 'Account',
                        'state': [{'name': 'accounts', 'schema': {'balance': 'int'}}],
                        'handlers': [
                            {
                                'name': 'Deposit',
                                'on': 'DepositMsg',
                                'logic': [
                                    {'read': 'accounts', 'as': 'acc', 'key': '1'},
                                    {'create': 'accounts', 'data': {'balance': '{{ non_existent_var + 100 }}'}}
                                ]
                            }
                        ]
                    }
                ]
            },
            'types': {'messages': {'DepositMsg': {'amount': 'int'}}}
        }
        
        analyzer = ClothoStaticAnalyzer(invalid_yaml)
        try:
            analyzer.analyze()
            self.fail("Should have raised ValueError for invalid variable")
        except ValueError as e:
            print(f"Caught expected error: {e}")

    def test_invariants(self):
        print("\n--- Testing Runtime Invariants ---")
        # Setup a simulator with a failing invariant
        clotho_data = {
            'design': {
                'components': [
                    {
                        'name': 'TestComp',
                        'state': [{'name': 'MyTable', 'schema': {'val': 'int'}}],
                        'invariants': [
                            # Use min() to handle list projection from read.TestComp.MyTable.val
                            {'name': 'PositiveVal', 'expression': 'min(read.TestComp.MyTable.val) >= 0'}
                        ],
                        'handlers': [
                            {
                                'on_message': 'SetVal', # Use on_message for simulator compatibility
                                'logic': [
                                    # Logic steps
                                    {'create': 'MyTable', 'data': {'val': 'trigger.payload.val'}}
                                ]
                            }
                        ]
                    }
                ]
            },
            'run': {
                'scenarios': [
                    {
                        'name': 'TestScenario',
                        'steps': [
                            {'send': 'SetVal', 'to': 'TestComp', 'payload': {'val': -5}}
                        ]
                    }
                ]
            }
        }
        
        sim = Simulator(clotho_data, config={'strict_invariants': True, 'db_mode': 'memory'})
        
        # Run the simulation step-by-step
        # sim.run() # Init
        
        try:
            sim.run()
            self.fail("Should have raised RuntimeError due to invariant failure")
        except RuntimeError as e:
            print(f"Caught expected invariant failure: {e}")
            self.assertIn("Invariant FAILED", str(e))

if __name__ == '__main__':
    unittest.main()
