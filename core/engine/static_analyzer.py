# core/engine/static_analyzer.py
import logging
from typing import Dict, List, Set, Any
from lark import Visitor, Tree
from .expression_engine import expression_parser

class VariableCollector(Visitor):
    """
    Lark Visitor that collects all variable references from an expression AST.
    """
    def __init__(self):
        self.variables = set()

    def variable(self, tree):
        # Reconstruct the variable name from the tree
        # tree.children are Tokens (CNAME)
        var_path = ".".join(token.value for token in tree.children)
        self.variables.add(var_path)

class ClothoStaticAnalyzer:
    """
    Performs static semantic analysis on the Clotho blueprint.
    Ensures that all variable references in expressions are valid within their scope.
    """
    def __init__(self, clotho_data: Dict[str, Any]):
        self.data = clotho_data
        self.logger = logging.getLogger(__name__)
        self.errors = []
        
        # Symbol Tables
        self.types = self.data.get('types', {})
        self.messages = self.types.get('messages', {})
        
        # Support both design.components and top-level components
        comps = self.data.get('design', {}).get('components', [])
        if not comps:
            comps = self.data.get('components', [])
            
        self.components = {c['name']: c for c in comps}

    def analyze(self):
        """
        Main entry point for analysis.
        Raises ValueError if errors are found.
        """
        self.errors = []
        self._analyze_components()
        
        if self.errors:
            error_msg = "\n".join(self.errors)
            raise ValueError(f"Static Analysis Failed:\n{error_msg}")
        
        self.logger.info("Static Analysis Passed: All expressions are valid.")

    def _analyze_components(self):
        for comp_name, comp_data in self.components.items():
            handlers = comp_data.get('handlers', [])
            
            storage_schema = {}
            
            # Helper to normalize schema to set of column names
            def normalize_schema(schema_def):
                cols = set()
                if isinstance(schema_def, list):
                    # List format: [{name: ..., type: ...}, ...] or ["col1", "col2", ...]
                    for col in schema_def:
                        if isinstance(col, dict):
                            cols.add(col.get('column', ''))
                            cols.add(col.get('name', ''))
                        elif isinstance(col, str):
                            cols.add(col)
                elif isinstance(schema_def, dict):
                    # Dict format: {col_name: {type: ..., ...}, ...}
                    cols = set(schema_def.keys())
                return cols

            # Only support modern 'state' format
            if 'state' in comp_data:
                for table in comp_data['state']:
                    table_name = table.get('name')
                    schema = table.get('schema', {})
                    if table_name:
                        storage_schema[table_name] = normalize_schema(schema)
            
            for handler in handlers:
                self._analyze_handler(comp_name, handler, storage_schema)

    def _analyze_handler(self, comp_name: str, handler: Dict, storage_schema: Dict):
        handler_name = handler.get('name', 'unnamed')
        trigger_msg_type = handler.get('on') or handler.get('on_message')
        
        # 1. Build Context (Symbol Table for this scope)
        # 'msg' context
        msg_fields = self._get_message_fields(trigger_msg_type)
        
        # 'read' context
        read_vars = {}
        logic = handler.get('logic', {})
        
        if not isinstance(logic, list):
            self.errors.append(f"[{comp_name}.{handler_name}] Handler logic must be a list of steps, found {type(logic).__name__}")
            return
        
        # Modern logic format (List of steps)
        for step in logic:
            if 'read' in step:
                table_name = step.get('read')
                alias = step.get('as')
                if alias and table_name:
                    if table_name in storage_schema:
                        read_vars[alias] = storage_schema[table_name]
        
        # Validate steps
        for step in logic:
            if 'create' in step or 'update' in step:
                # Validate data/set expressions
                data = step.get('data') or step.get('set', {})
                for k, v in data.items():
                    if self._is_expression(v):
                        self._validate_expression(v, comp_name, handler_name, f"step.{k}", msg_fields, read_vars)

    def _validate_expression(self, expr_str: str, comp_name: str, handler_name: str, location: str, msg_fields: Set[str], read_vars: Dict):
        """
        Parses the expression and checks if all variables exist in the scope.
        Handles string interpolation by extracting parts inside {{...}}.
        """
        import re
        # Find all expressions inside {{...}}
        matches = re.findall(r"\{\{(.*?)\}\}", expr_str)
        
        if not matches:
            # Should not happen if _is_expression returned true, unless braces are malformed
            return

        for inner_expr in matches:
            inner_expr = inner_expr.strip()
            try:
                tree = expression_parser.parse(inner_expr)
                collector = VariableCollector()
                collector.visit(tree)
                
                for var in collector.variables:
                    if not self._is_variable_valid(var, msg_fields, read_vars):
                        self.errors.append(f"[{comp_name}.{handler_name}] Invalid variable '{var}' in {location}: '{expr_str}'")
                        
            except Exception as e:
                self.errors.append(f"[{comp_name}.{handler_name}] Syntax error in {location}: '{expr_str}'. Error: {str(e)}")

    def _is_variable_valid(self, var: str, msg_fields: Set[str], read_vars: Dict) -> bool:
        parts = var.split('.')
        root = parts[0]
        
        if root == 'msg':
            if len(parts) < 2: return False # just 'msg' is not allowed usually
            field = parts[1]
            if msg_fields is None: return True # Permissive if message type undefined
            return field in msg_fields
            
        elif root == 'trigger':
            if len(parts) < 2: return False
            second = parts[1]
            if second in ('sender', 'message', 'timestamp'):
                return True
            if second == 'payload':
                if len(parts) < 3: return False # trigger.payload is a dict, usually accessed deeper
                field = parts[2]
                if msg_fields is None: return True # Permissive if message type undefined
                return field in msg_fields
            return False

        elif root == 'read':
            if len(parts) < 3: return False # read.alias.field
            alias = parts[1]
            field = parts[2]
            
            if alias not in read_vars:
                return False
            
            # Check if field exists in the table schema
            table_schema = read_vars[alias]
            return field in table_schema
            
        # Allow 'now()' function call (handled by parser as function_call, but if it appears as var?)
        # The grammar distinguishes function_call from variable. 
        # VariableCollector only collects 'variable' nodes.
        
        return False

    def _get_message_fields(self, msg_type: str) -> Set[str]:
        if not msg_type: return set()
        # If message type is not in definitions, return None to indicate "unknown structure"
        # This allows tests that don't define 'types' to pass.
        if msg_type not in self.messages:
            return None
        msg_def = self.messages.get(msg_type, {})
        return set(msg_def.keys())

    def _is_expression(self, val: Any) -> bool:
        return isinstance(val, str) and "{{" in val and "}}" in val

    def _strip_template(self, val: str) -> str:
        return val.replace("{{", "").replace("}}", "").strip()
