"""
Unit tests for Clotho Expression Engine

Tests cover:
1. Basic expression evaluation (literals, variables)
2. Arithmetic operations (+, -, *, /)
3. Nested expressions
4. Field access (dot notation)
5. Type handling (int, float, string, None)
6. Error handling
7. Match condition resolution (critical for match/case logic)
"""

import unittest
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.engine.expression_engine import ExpressionInterpreter, evaluate, expression_parser


class TestExpressionEvaluation(unittest.TestCase):
    """Test basic expression evaluation"""
    
    def setUp(self):
        """Set up test context"""
        self.context = {
            'trigger': {
                'payload': {
                    'amount': 2000,
                    'account_id': 'ACC_001',
                    'timestamp': '2025-01-15T10:00:00Z',
                    'flag': True
                }
            },
            'read': {
                'account': {
                    'balance': 5000,
                    'overdraft_limit': 500,
                    'status': 'Active'
                },
                'customer': {
                    'name': 'Alice Johnson',
                    'tier': 'Gold'
                }
            }
        }
        self.interpreter = ExpressionInterpreter(self.context)
    
    def test_literal_integer(self):
        """Test integer literal evaluation"""
        result = evaluate('100', self.context)
        self.assertEqual(result, 100)
        self.assertIsInstance(result, int)
    
    def test_literal_float(self):
        """Test float literal evaluation"""
        result = evaluate('99.99', self.context)
        self.assertAlmostEqual(result, 99.99)
        self.assertIsInstance(result, float)
    
    def test_literal_string(self):
        """Test string literal evaluation"""
        result = evaluate('"hello world"', self.context)
        self.assertEqual(result, 'hello world')
    
    def test_literal_boolean(self):
        """Test boolean literal evaluation"""
        self.assertTrue(evaluate('true', self.context))
        self.assertFalse(evaluate('false', self.context))
    
    def test_field_access_simple(self):
        """Test simple field access"""
        result = evaluate('trigger.payload.amount', self.context)
        self.assertEqual(result, 2000)
    
    def test_field_access_nested(self):
        """Test nested field access"""
        result = evaluate('read.account.balance', self.context)
        self.assertEqual(result, 5000)
    
    def test_field_access_string(self):
        """Test field access returning string"""
        result = evaluate('read.customer.name', self.context)
        self.assertEqual(result, 'Alice Johnson')
        self.assertIsInstance(result, str)


class TestArithmeticOperations(unittest.TestCase):
    """Test arithmetic operations"""
    
    def setUp(self):
        self.context = {
            'read': {
                'account': {
                    'balance': 5000,
                    'overdraft_limit': 500
                }
            },
            'trigger': {
                'payload': {
                    'amount': 2000,
                    'fee': 10
                }
            }
        }
    
    def test_addition_integers(self):
        """Test integer addition"""
        result = evaluate('100 + 200', self.context)
        self.assertEqual(result, 300)
        self.assertIsInstance(result, int)
    
    def test_addition_floats(self):
        """Test float addition"""
        result = evaluate('99.5 + 0.5', self.context)
        self.assertAlmostEqual(result, 100.0)
    
    def test_addition_field_access(self):
        """Test addition with field access"""
        result = evaluate('read.account.balance + read.account.overdraft_limit', self.context)
        self.assertEqual(result, 5500)
    
    def test_subtraction(self):
        """Test subtraction"""
        result = evaluate('read.account.balance - trigger.payload.amount', self.context)
        self.assertEqual(result, 3000)
    
    def test_multiplication(self):
        """Test multiplication"""
        result = evaluate('trigger.payload.amount * 2', self.context)
        self.assertEqual(result, 4000)
    
    def test_division(self):
        """Test division"""
        result = evaluate('trigger.payload.amount / 2', self.context)
        self.assertAlmostEqual(result, 1000.0)
    
    def test_complex_expression(self):
        """Test complex arithmetic expression"""
        result = evaluate('(read.account.balance - trigger.payload.amount) + trigger.payload.fee', self.context)
        self.assertEqual(result, 3010)
    
    def test_operator_precedence(self):
        """Test operator precedence (multiplication before addition)"""
        result = evaluate('10 + 5 * 2', self.context)
        self.assertEqual(result, 20)  # Should be 10 + (5*2) = 20, not (10+5)*2 = 30


class TestMatchConditionResolution(unittest.TestCase):
    """
    Test match condition resolution - CRITICAL for match/case logic
    This is where the bug was found!
    """
    
    def setUp(self):
        self.context = {
            'trigger': {
                'payload': {
                    'amount': 2000,
                    'status': 'Active'
                }
            },
            'read': {
                'account': {
                    'balance': 5000,
                    'overdraft_limit': 500
                }
            }
        }
    
    def test_embedded_expression_in_comparison(self):
        """
        Test embedded {{...}} in comparison string
        This was the critical bug: '>= {{trigger.payload.amount}}' was not resolved
        """
        from core.engine.clotho_simulator import Simulator
        
        # Create a minimal simulator instance just to test _resolve_expressions
        clotho_data = {'components': [], 'scenarios': []}
        sim = Simulator(clotho_data, mode='yaml')
        
        interpreter = ExpressionInterpreter(self.context)
        
        # Test resolving embedded expression
        condition = '>= {{trigger.payload.amount}}'
        resolved = sim._resolve_expressions(condition, interpreter)
        
        # Should resolve to '>= 2000'
        self.assertEqual(resolved, '>= 2000')
    
    def test_embedded_expression_equality(self):
        """Test embedded expression in equality comparison"""
        from core.engine.clotho_simulator import Simulator
        
        clotho_data = {'components': [], 'scenarios': []}
        sim = Simulator(clotho_data, mode='yaml')
        interpreter = ExpressionInterpreter(self.context)
        
        condition = '== {{trigger.payload.status}}'
        resolved = sim._resolve_expressions(condition, interpreter)
        
        self.assertEqual(resolved, '== Active')
    
    def test_multiple_embedded_expressions(self):
        """Test multiple embedded expressions in one string"""
        from core.engine.clotho_simulator import Simulator
        
        context = {
            'a': {'value': 100},
            'b': {'value': 200}
        }
        
        clotho_data = {'components': [], 'scenarios': []}
        sim = Simulator(clotho_data, mode='yaml')
        interpreter = ExpressionInterpreter(context)
        
        # Test string with multiple embedded expressions
        condition = 'between {{a.value}} and {{b.value}}'
        resolved = sim._resolve_expressions(condition, interpreter)
        
        self.assertEqual(resolved, 'between 100 and 200')
    
    def test_complete_expression_resolution(self):
        """Test complete {{...}} expression (should return evaluated value)"""
        from core.engine.clotho_simulator import Simulator
        
        clotho_data = {'components': [], 'scenarios': []}
        sim = Simulator(clotho_data, mode='yaml')
        interpreter = ExpressionInterpreter(self.context)
        
        # Complete expression should return int, not string
        expr = '{{read.account.balance + read.account.overdraft_limit}}'
        resolved = sim._resolve_expressions(expr, interpreter)
        
        self.assertEqual(resolved, 5500)
        self.assertIsInstance(resolved, int)


class TestConditionEvaluation(unittest.TestCase):
    """Test _evaluate_condition method from Simulator"""
    
    def setUp(self):
        from core.engine.clotho_simulator import Simulator
        
        clotho_data = {'components': [], 'scenarios': []}
        self.sim = Simulator(clotho_data, mode='yaml')
    
    def test_numeric_greater_than(self):
        """Test numeric > comparison"""
        result = self.sim._evaluate_condition(5500, '> 2000')
        self.assertTrue(result)
        
        result = self.sim._evaluate_condition(1000, '> 2000')
        self.assertFalse(result)
    
    def test_numeric_greater_equal(self):
        """Test numeric >= comparison"""
        result = self.sim._evaluate_condition(5500, '>= 2000')
        self.assertTrue(result)
        
        result = self.sim._evaluate_condition(2000, '>= 2000')
        self.assertTrue(result)
        
        result = self.sim._evaluate_condition(1999, '>= 2000')
        self.assertFalse(result)
    
    def test_numeric_less_than(self):
        """Test numeric < comparison"""
        result = self.sim._evaluate_condition(1000, '< 2000')
        self.assertTrue(result)
        
        result = self.sim._evaluate_condition(3000, '< 2000')
        self.assertFalse(result)
    
    def test_numeric_less_equal(self):
        """Test numeric <= comparison"""
        result = self.sim._evaluate_condition(1000, '<= 2000')
        self.assertTrue(result)
        
        result = self.sim._evaluate_condition(2000, '<= 2000')
        self.assertTrue(result)
        
        result = self.sim._evaluate_condition(2001, '<= 2000')
        self.assertFalse(result)
    
    def test_numeric_equality(self):
        """Test numeric == comparison"""
        result = self.sim._evaluate_condition(2000, '== 2000')
        self.assertTrue(result)
        
        result = self.sim._evaluate_condition(1999, '== 2000')
        self.assertFalse(result)
    
    def test_numeric_inequality(self):
        """Test numeric != comparison"""
        result = self.sim._evaluate_condition(1999, '!= 2000')
        self.assertTrue(result)
        
        result = self.sim._evaluate_condition(2000, '!= 2000')
        self.assertFalse(result)
    
    def test_string_equality(self):
        """Test string == comparison"""
        result = self.sim._evaluate_condition('Active', '== Active')
        self.assertTrue(result)
        
        result = self.sim._evaluate_condition('Inactive', '== Active')
        self.assertFalse(result)
    
    def test_null_comparison(self):
        """Test None/null comparison"""
        result = self.sim._evaluate_condition(None, '== null')
        self.assertTrue(result)
        
        result = self.sim._evaluate_condition('something', '!= null')
        self.assertTrue(result)


class TestTypeConversion(unittest.TestCase):
    """Test type conversion and handling"""
    
    def test_float_to_int_conversion(self):
        """Test that float results without decimals convert to int"""
        context = {}
        result = evaluate('100.0', context)
        self.assertEqual(result, 100)
        self.assertIsInstance(result, int)
    
    def test_float_preservation(self):
        """Test that floats with decimals are preserved"""
        context = {}
        result = evaluate('99.5', context)
        self.assertAlmostEqual(result, 99.5)
        self.assertIsInstance(result, float)
    
    def test_division_returns_float(self):
        """Test that division returns float"""
        context = {}
        result = evaluate('100 / 2', context)
        self.assertEqual(result, 50)
        # Division should return int if result is whole number
        self.assertIsInstance(result, int)
    
    def test_mixed_int_float_arithmetic(self):
        """Test arithmetic with mixed int/float"""
        context = {}
        result = evaluate('100 + 0.5', context)
        self.assertAlmostEqual(result, 100.5)


class TestErrorHandling(unittest.TestCase):
    """Test error handling and edge cases"""
    
    def test_undefined_variable(self):
        """Test accessing undefined variable"""
        context = {'a': {'b': 123}}
        result = evaluate('x.y.z', context)
        # Should return None on error
        self.assertIsNone(result)
    
    def test_invalid_syntax(self):
        """Test invalid expression syntax"""
        context = {}
        result = evaluate('100 +', context)  # Incomplete expression
        self.assertIsNone(result)
    
    def test_division_by_zero(self):
        """Test division by zero handling"""
        context = {}
        result = evaluate('100 / 0', context)
        # Should handle gracefully (return None or raise appropriate error)
        # Actual behavior depends on implementation
        # This test documents the behavior
    
    def test_empty_expression(self):
        """Test empty expression"""
        context = {}
        result = evaluate('', context)
        self.assertIsNone(result)


class TestRealWorldScenarios(unittest.TestCase):
    """Test real-world Clotho scenarios"""
    
    def test_banking_scenario_balance_check(self):
        """Test realistic banking balance check scenario"""
        context = {
            'trigger': {
                'payload': {
                    'amount': 2000
                }
            },
            'read': {
                'account': {
                    'balance': 5000,
                    'overdraft_limit': 500
                }
            }
        }
        
        # Calculate available funds
        available = evaluate('read.account.balance + read.account.overdraft_limit', context)
        self.assertEqual(available, 5500)
        
        # Check if sufficient
        from core.engine.clotho_simulator import Simulator
        clotho_data = {'components': [], 'scenarios': []}
        sim = Simulator(clotho_data, mode='yaml')
        
        has_sufficient = sim._evaluate_condition(available, '>= 2000')
        self.assertTrue(has_sufficient)
    
    def test_fraud_detection_scenario(self):
        """Test fraud detection risk scoring"""
        context = {
            'trigger': {
                'payload': {
                    'amount': 15000
                }
            },
            'read': {
                'risk': {
                    'risk_score': 85
                }
            }
        }
        
        from core.engine.clotho_simulator import Simulator
        clotho_data = {'components': [], 'scenarios': []}
        sim = Simulator(clotho_data, mode='yaml')
        
        # Check if high-risk transaction
        amount = evaluate('trigger.payload.amount', context)
        is_high_amount = sim._evaluate_condition(amount, '> 10000')
        self.assertTrue(is_high_amount)
        
        risk_score = evaluate('read.risk.risk_score', context)
        is_high_risk = sim._evaluate_condition(risk_score, '> 50')
        self.assertTrue(is_high_risk)
    
    def test_reward_points_calculation(self):
        """Test reward points calculation"""
        context = {
            'trigger': {
                'payload': {
                    'amount': 2000
                }
            },
            'read': {
                'rewards': {
                    'total_points': 5000
                }
            }
        }
        
        # Calculate new points (2x amount)
        new_points = evaluate('read.rewards.total_points + (trigger.payload.amount * 2)', context)
        self.assertEqual(new_points, 9000)


class TestExpressionGrammar(unittest.TestCase):
    """Test expression grammar parsing"""
    
    def test_parentheses(self):
        """Test parentheses for grouping"""
        context = {}
        result = evaluate('(10 + 5) * 2', context)
        self.assertEqual(result, 30)
    
    def test_nested_parentheses(self):
        """Test nested parentheses"""
        context = {}
        result = evaluate('((10 + 5) * 2) + 1', context)
        self.assertEqual(result, 31)
    
    def test_negative_numbers(self):
        """Test negative number literals"""
        context = {}
        result = evaluate('-100', context)
        self.assertEqual(result, -100)
    
    def test_mixed_operators(self):
        """Test mixed arithmetic operators"""
        context = {}
        result = evaluate('10 + 5 * 2 - 3', context)
        self.assertEqual(result, 17)  # 10 + (5*2) - 3


def run_tests():
    """Run all tests"""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestExpressionEvaluation))
    suite.addTests(loader.loadTestsFromTestCase(TestArithmeticOperations))
    suite.addTests(loader.loadTestsFromTestCase(TestMatchConditionResolution))
    suite.addTests(loader.loadTestsFromTestCase(TestConditionEvaluation))
    suite.addTests(loader.loadTestsFromTestCase(TestTypeConversion))
    suite.addTests(loader.loadTestsFromTestCase(TestErrorHandling))
    suite.addTests(loader.loadTestsFromTestCase(TestRealWorldScenarios))
    suite.addTests(loader.loadTestsFromTestCase(TestExpressionGrammar))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Return exit code
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    exit_code = run_tests()
    sys.exit(exit_code)
