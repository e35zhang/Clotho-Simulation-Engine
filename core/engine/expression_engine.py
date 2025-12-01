# core/expression_engine.py
import uuid
import os
from functools import lru_cache
from lark import Lark, Transformer, v_args, Token # Import Token

# 1. Build an absolute path to the grammar file
script_dir = os.path.dirname(__file__)
grammar_path = os.path.join(script_dir, 'expression_grammar.lark')
with open(grammar_path, 'r') as f:
    grammar = f.read()

# --- DEFINE PARSER GLOBALLY HERE ---
# 2. Create the Lark parser instance
expression_parser = Lark(grammar, start='expression')

# 2.1 Caching Wrapper (Fix #4: Performance)
@lru_cache(maxsize=4096)
def cached_parse(expression_string):
    return expression_parser.parse(expression_string)

# 3. Create the Interpreter (AST Transformer)
@v_args(inline=True)
class ExpressionInterpreter(Transformer):
    def __init__(self, context):
        self.context = context
        super().__init__()

    # --- Operator handlers (Handling Named Operator Tokens) ---
    def logical_or(self, *args):
        if len(args) == 1: return args[0]
        left = args[0]
        i = 1
        while i < len(args):
            # Short-circuit evaluation
            if left: return True
            right = args[i+1]
            left = left or right
            i += 2
        return left

    def logical_and(self, *args):
        if len(args) == 1: return args[0]
        left = args[0]
        i = 1
        while i < len(args):
            # Short-circuit evaluation
            if not left: return False
            right = args[i+1]
            left = left and right
            i += 2
        return left

    def comparison(self, *args):
        if len(args) == 1: return args[0]
        left = args[0]
        i = 1
        while i < len(args):
            op_token = args[i]
            right = args[i+1]
            op = op_token.type
            
            # Fix #2: Null Safety - Explicit handling for None in comparisons
            # If either side is None, standard comparisons (>, <, >=, <=) are False (or None?)
            # Equality (==, !=) works fine with None.
            
            if op in ('GT', 'LT', 'GTE', 'LTE'):
                if left is None or right is None:
                    return False # Treat as False, similar to SQL NULL behavior in WHERE (mostly)

            try:
                if op == 'EQ': left = (left == right)
                elif op == 'NEQ': left = (left != right)
                elif op == 'GT': left = (float(left) > float(right))
                elif op == 'LT': left = (float(left) < float(right))
                elif op == 'GTE': left = (float(left) >= float(right))
                elif op == 'LTE': left = (float(left) <= float(right))
                elif op == 'IN': left = (left in right)
            except (ValueError, TypeError):
                # If comparison fails (e.g. string vs int), return False or handle gracefully
                # For now, strict comparison failure -> False
                return False
            i += 2
        return left

    def addition(self, *args):
        # args will now be like:
        # (result_A,)
        # (result_A, Token(type='SUB', value='-'), result_B)
        
        if len(args) == 1:
             return args[0]

        left_result = args[0]
        i = 1
        while i < len(args):
            op_token = args[i]
            if i + 1 >= len(args):
                 return None
            right_result = args[i+1]

            # Fix #2: Null Safety - Propagate None gracefully
            if left_result is None or right_result is None:
                return None

            op = None
            # Check the TYPE of the token now
            if isinstance(op_token, Token) and op_token.type in ('ADD', 'SUB'):
                op = op_token.value # Get '+' or '-'
            else:
                return None

            try:
                current_left = float(left_result)
                current_right = float(right_result)
            except (ValueError, TypeError):
                return None

            if op == '+':
                left_result = current_left + current_right
            elif op == '-':
                left_result = current_left - current_right

            i += 2
        return left_result


    def multiplication(self, *args):
        if len(args) == 1:
            return args[0]

        left_result = args[0]
        i = 1
        while i < len(args):
            op_token = args[i]
            if i + 1 >= len(args):
                 return None
            right_result = args[i+1]

            # Fix #2: Null Safety - Propagate None gracefully
            if left_result is None or right_result is None:
                return None

            op = None
            # Check the TYPE of the token
            if isinstance(op_token, Token) and op_token.type in ('MUL', 'DIV'):
                op = op_token.value # Get '*' or '/'
            else:
                return None

            try:
                num_left = float(left_result)
                num_right = float(right_result)
            except (ValueError, TypeError):
                return None

            if op == '*':
                left_result = num_left * num_right
            elif op == '/':
                if num_right == 0:
                    return None
                left_result = num_left / num_right

            i += 2
        return left_result

    # --- Literal value handlers ---
    def number(self, n): return float(n)
    def string(self, s): return s[1:-1]
    def true_lit(self, *args): return True
    def false_lit(self, *args): return False
    def null_lit(self, *args): return None
    def list_literal(self, *args): return list(args)

    # --- Variable and Function handlers ---
    def variable(self, *parts):
        value = self.context
        try:
            for part in parts:
                key_to_lookup = part.value
                
                # Handle list projection: [obj1, obj2].field -> [obj1.field, obj2.field]
                if isinstance(value, list):
                    new_value = []
                    for item in value:
                        if isinstance(item, dict):
                            val = item.get(key_to_lookup)
                            new_value.append(val)
                        elif hasattr(item, key_to_lookup):
                            val = getattr(item, key_to_lookup)
                            new_value.append(val)
                    value = new_value
                    continue

                if isinstance(value, dict):
                    value = value.get(key_to_lookup)
                elif hasattr(value, key_to_lookup):
                     value = getattr(value, key_to_lookup)
                else:
                    return None
                if value is None:
                    break
            return value
        except (AttributeError, TypeError) as e:
            print(f"[INTERPRETER-ERROR] Lookup failed during access: {e}")
            return None

    def function_call(self, name, *args):
        func_name = name.value
        if func_name == 'uuid':
            return uuid.uuid4().hex
        elif func_name == 'sum':
            # Expect a list
            if len(args) == 1 and isinstance(args[0], list):
                return sum(args[0])
            return sum(args)
        elif func_name == 'all':
            if len(args) == 1 and isinstance(args[0], list):
                return all(args[0])
            return all(args)
        elif func_name == 'any':
            if len(args) == 1 and isinstance(args[0], list):
                return any(args[0])
            return any(args)
        elif func_name == 'len':
            if len(args) == 1:
                return len(args[0])
            return 0
        elif func_name == 'min':
            if len(args) == 1 and isinstance(args[0], list):
                if not args[0]: return None # Empty list
                return min(args[0])
            return min(args)
        elif func_name == 'max':
            if len(args) == 1 and isinstance(args[0], list):
                if not args[0]: return None # Empty list
                return max(args[0])
            return max(args)
        else:
            # Try to find function in context (e.g. custom functions)
            # For now, just raise error
            raise NameError(f"Function '{func_name}' is not defined.")


# --- evaluate function using global expression_parser ---
def evaluate(expression_string: str, context_or_interpreter) -> any:
    if not isinstance(expression_string, str):
        return expression_string # Return non-strings as is

    interpreter = None
    if isinstance(context_or_interpreter, ExpressionInterpreter):
        interpreter = context_or_interpreter
    elif isinstance(context_or_interpreter, dict):
        interpreter = ExpressionInterpreter(context_or_interpreter)
    else:
        # print(f"[EXPRESSION-ERROR] Invalid context type for evaluate: {type(context_or_interpreter)}")
        return None

    try:
        # print(f"[ENGINE-DEBUG] Evaluating: '{expression_string}' with context keys: {list(interpreter.context.keys())}") # Use list() for clarity
        
        # Use cached parser (Fix #4)
        tree = cached_parse(expression_string)
        
        # print(f"[ENGINE-DEBUG] Parsed AST:\n{tree.pretty()}") # Keep commented unless debugging AST

        result = interpreter.transform(tree)

        # Convert numeric results back to int if they don't have a decimal part
        if isinstance(result, float) and result.is_integer():
            result = int(result)
        # print(f"[ENGINE-DEBUG] Final result: {result} (type: {type(result)})")
        return result
    except Exception as e:
        # Include the expression string in the error message for better context
        # print(f"[EXPRESSION-ERROR] Failed to evaluate '{expression_string}': {e}")
        return None # Return None on evaluation error
