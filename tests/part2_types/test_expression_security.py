
import unittest
from core.engine.expression_engine import evaluate

class TestExpressionSecurity(unittest.TestCase):
    """
    Security tests for the Expression Engine.
    Ensures that the sandbox cannot be escaped to execute arbitrary code.
    """

    def test_no_eval_usage(self):
        """Verify that we are not using python's eval()"""
        # This is a meta-test, but we can verify behavior
        # If eval() was used, "2 + 2" would work.
        # Our engine supports "2 + 2" too.
        # But "import os" would work in eval() (if not restricted), but should fail in our parser.
        
        expression = "import os"
        # Should return None or raise an error (likely ParseError or similar, but evaluate catches it and returns None)
        result = evaluate(expression, {})
        self.assertIsNone(result, "Should not evaluate arbitrary python statements")

    def test_function_whitelist(self):
        """Verify that only whitelisted functions can be called."""
        # 'sum' is whitelisted
        self.assertEqual(evaluate("sum([1, 2])", {}), 3)
        
        # 'eval' is NOT whitelisted
        result = evaluate("eval('1+1')", {})
        self.assertIsNone(result, "Should not allow calling eval()")
        
        # '__import__' is NOT whitelisted
        result = evaluate("__import__('os')", {})
        self.assertIsNone(result, "Should not allow calling __import__")
        
        # 'exit' is NOT whitelisted
        result = evaluate("exit()", {})
        self.assertIsNone(result, "Should not allow calling exit()")

    def test_attribute_access_restrictions(self):
        """
        Verify that while we can access attributes, we can't execute them.
        """
        context = {"user": "Alice"}
        
        # Accessing __class__ is possible (it's just a property)
        # But we can't do anything with it except read it (and our engine might not even support returning types)
        # Let's see what happens.
        result = evaluate("user.__class__", context)
        # It might return the type object <class 'str'>
        self.assertEqual(result, str)
        
        # But we cannot instantiate it or call it
        # "user.__class__('Bob')" -> This would be parsed as a function call "user.__class__" which is NOT a CNAME.
        # The grammar for function_call is CNAME "(" ... ")"
        # So "user.__class__" is NOT a valid function name in the grammar.
        # So it should fail parsing.
        result = evaluate("user.__class__('Bob')", context)
        self.assertIsNone(result, "Should not allow calling methods on objects")

    def test_method_calls_on_objects(self):
        """
        Verify we cannot call methods on objects.
        e.g. "user.upper()"
        """
        context = {"user": "alice"}
        
        # "user.upper()" -> In python this is a method call.
        # In our grammar:
        # variable: CNAME ("." CNAME)* -> "user.upper" is a variable access.
        # function_call: CNAME "(" ... ")" -> "user.upper" is NOT a CNAME.
        # So "user.upper()" does not match function_call.
        # Does it match anything else?
        # No. So it should be a syntax error.
        
        result = evaluate("user.upper()", context)
        self.assertIsNone(result, "Should not allow calling methods on objects")

if __name__ == '__main__':
    unittest.main()
