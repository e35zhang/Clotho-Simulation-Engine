# core/clotho_simulator.py
import sqlite3
import uuid
import os
import re
import yaml
import json
import importlib
import random
import logging
from datetime import datetime, timezone
# --- IMPORT PARSER, INTERPRETER, and EVALUATE function ---
from .expression_engine import expression_parser, ExpressionInterpreter, evaluate # <-- ADD evaluate
from .static_analyzer import ClothoStaticAnalyzer # Import Static Analyzer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Proxy Classes for Lazy Loading in Expressions ---
class ComponentProxy:
    def __init__(self, simulator, component_name):
        self.sim = simulator
        self.component_name = component_name
    
    def __getattr__(self, table_name):
        return self.sim._read_full_table(self.component_name, table_name)

class RootProxy:
    def __init__(self, simulator):
        self.sim = simulator
    
    def __getattr__(self, component_name):
        return ComponentProxy(self.sim, component_name)

class Simulator:
    # --- MODIFIED __init__ ---
    def __init__(self, clotho_data, mode='yaml', python_module_name=None, target_component=None, simulation_seed=None, config=None):
        self.logger = logging.getLogger(f"{__name__}.Simulator")
        
        # --- STATIC ANALYSIS (Fix #1) ---
        # Run static analysis before initializing anything else
        # This ensures we fail fast on invalid variable references
        try:
            analyzer = ClothoStaticAnalyzer(clotho_data)
            analyzer.analyze()
        except ValueError as e:
            self.logger.error(f"Static Analysis Failed: {e}")
            raise # Re-raise to stop initialization
            
        self.clotho_data = clotho_data
        self.config = config or {}
        
        # Configuration
        self.max_events = self.config.get('max_events', 100000) # Default increased to 100k
        self.db_mode = self.config.get('db_mode', 'file') # 'file' or 'memory'
        
        # M3: Randomized scheduling with seed-based replay
        # CRITICAL FIX: Use private Random instance to avoid thread pollution
        self.simulation_seed = simulation_seed if simulation_seed is not None else random.randint(0, 2**32 - 1)
        self.rng = random.Random(self.simulation_seed)  # Thread-safe private RNG
        self.logger.info(f"Initialized with simulation seed: {self.simulation_seed}")
        
        # DETERMINISTIC ID GENERATION: Use self.rng instead of uuid.uuid4()
        if self.db_mode == 'memory':
            self.db_path = ":memory:"
        else:
            self.db_path = f"run_{self._generate_deterministic_id('db', 8)}.sqlite"
        
        self.conn = None
        self.cursor = None
        self.current_scenario = None
        self.event_queue = []
        self.processed_event_count = 0 # Track processed events

        # --- NEW properties for Verification Mode ---
        self.mode = mode
        self.target_component = target_component # The one component to run in Python
        self.py_handler_instance = None

        # Context for interface methods
        self._current_cid = None
        self._current_owner = None
        self._current_event_id = None  # Track current event ID for causation chain
        self._current_causation_id = None  # Track parent event ID (causation)

        # --- Clotho Structure Support ---
        if 'design' in self.clotho_data and 'components' in self.clotho_data['design']:
            self.clotho_data['components'] = self.clotho_data['design']['components']
        
        # Handle scenarios in 'run' section (Clotho v3)
        if 'scenarios' not in self.clotho_data and 'run' in self.clotho_data:
            if 'scenarios' in self.clotho_data['run']:
                self.clotho_data['scenarios'] = self.clotho_data['run']['scenarios']
            else:
                self.clotho_data['scenarios'] = self._synthesize_scenarios_from_run(self.clotho_data['run'])

        if self.mode == 'python':
            if not python_module_name or not target_component:
                raise ValueError("Python mode requires 'python_module_name' and 'target_component'")
            self._load_python_handler(python_module_name, target_component)
    
    def _generate_deterministic_id(self, prefix="evt", length=12):
        """Generate deterministic ID based on self.rng instead of uuid.uuid4()
        
        This ensures that with the same simulation seed, all IDs are reproducible.
        Critical for deterministic replay and trace comparison.
        
        Args:
            prefix: ID prefix (e.g., 'evt', 'tx', 'db')
            length: Hex string length (default 12)
        
        Returns:
            Deterministic ID string like 'evt_a1b2c3d4e5f6'
        """
        # Generate random bits using self.rng (controlled by simulation_seed)
        rand_bits = self.rng.getrandbits(length * 4)  # 4 bits per hex char
        return f"{prefix}_{rand_bits:0{length}x}"

    # --- Load the AI-generated code ---
    def _load_python_handler(self, module_name, component_name):
        self.logger.info(f"Loading Python handler for '{component_name}' from module '{module_name}'")
        try:
            # Dynamically import the module
            handler_module = importlib.import_module(module_name)
            # The class name is defined by our prompt
            HandlerClass = getattr(handler_module, f"{component_name}Handler")

            # Inject self as the state_manager and message_sender
            self.py_handler_instance = HandlerClass(state_manager=self, message_sender=self)
            self.logger.info("Python handler loaded and instantiated successfully.")
        except Exception as e:
            self.logger.error(f"Failed to load Python handler: {e}")
            raise

    def _connect_db(self, reset=True):
        if self.db_mode != 'memory' and reset and os.path.exists(self.db_path): 
            try:
                os.remove(self.db_path)
            except PermissionError:
                self.logger.warning(f"Could not remove existing DB file {self.db_path}, it might be in use.")
                
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

    def _close_db(self):
        """CRITICAL FIX: Robust DB closing to prevent file handle leaks"""
        if self.conn:
            try:
                self.conn.commit()
            except Exception:
                pass  # Ignore commit errors on close
            try:
                self.conn.close()
                self.conn = None
                self.cursor = None
            except Exception:
                pass  # Ignore close errors

    def _create_database_schema(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS event_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                timestamp TEXT NOT NULL,
                correlation_id TEXT NOT NULL,
                causation_id TEXT,
                component TEXT NOT NULL,
                handler_name TEXT,
                trigger_message TEXT,
                table_name TEXT NOT NULL,
                action TEXT NOT NULL,
                row_id TEXT,
                payload TEXT NOT NULL,
                simulation_seed INTEGER
            )
        """)
        
        # M3: Create simulation_metadata table to store seed and run info
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS simulation_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                simulation_seed INTEGER NOT NULL,
                scenario_name TEXT NOT NULL,
                start_timestamp TEXT NOT NULL,
                end_timestamp TEXT,
                event_count INTEGER DEFAULT 0
            )
        """)
        for comp_data in self.clotho_data.get('components', []):
            # Clotho: 'state' is a list of tables
            # We need to handle both formats
            
            tables_to_create = {}
            
            # Clotho format
            if 'state' in comp_data:
                for state_item in comp_data['state']:
                    tables_to_create[state_item['name']] = state_item['schema']

            for table_name, columns in tables_to_create.items():
                if not columns: continue
                full_table_name = f"{comp_data['name']}_{table_name}"
                cols = []
                
                # Clotho: columns is a dict {col_name: {type: ..., pk: ...}} or {col_name: type_str}
                if isinstance(columns, dict):
                    for col_name, col_def in columns.items():
                        # Handle simple type string: "balance: Money"
                        if isinstance(col_def, str):
                            raw_type = col_def
                            col_props = {}
                        else:
                            raw_type = col_def.get('type', 'string')
                            col_props = col_def
                            
                        # Check if it's a custom type defined in 'types'
                        # For now, map known primitives
                        sql_type = 'TEXT'
                        if raw_type in ['integer', 'Money', 'int', 'Counter']: 
                            sql_type = 'INTEGER'
                        elif raw_type in ['float', 'number']:
                            sql_type = 'REAL'
                        
                        col_str = f"{col_name} {sql_type}"
                        if col_props.get('pk') or col_name == 'id': # Auto-detect 'id' as PK for simplicity if not specified
                            col_str += " PRIMARY KEY"
                        elif col_props.get('not_null'):
                            col_str += " NOT NULL"
                        cols.append(col_str)

                try:
                    self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {full_table_name} ({', '.join(cols)})")
                except sqlite3.Error as e:
                    self.logger.error(f"Failed to create table {full_table_name}: {e}")
                    raise
        self.conn.commit()

    def _initialize_database_state(self):
        for state in self.current_scenario.get('initial_state', []):
            comp_name = state['component']
            # Support both 'state' and 'storage' keys for backwards compatibility
            storage_data = state.get('storage', state.get('state', {}))
            for table, records in storage_data.items():
                full_table_name = f"{comp_name}_{table}"
                for record in records:
                    cols = ', '.join(record.keys())
                    placeholders = ', '.join(['?'] * len(record))
                    sql = f"INSERT INTO {full_table_name} ({cols}) VALUES ({placeholders})"
                    try:
                        self.cursor.execute(sql, tuple(record.values()))
                    except sqlite3.Error as e:
                         self.logger.error(f"Failed to insert initial state into {full_table_name}: {e}\nRecord: {record}")
        self.conn.commit()

    # --- MODIFIED: Call evaluate without parser argument ---
    def _resolve_expressions(self, data_struct, context_or_interpreter):
        interpreter = None
        if isinstance(context_or_interpreter, ExpressionInterpreter):
             interpreter = context_or_interpreter
        elif isinstance(context_or_interpreter, dict):
             interpreter = ExpressionInterpreter(context_or_interpreter)
        else:
             self.logger.error(f"Invalid context type for resolution: {type(context_or_interpreter)}")
             return data_struct

        if isinstance(data_struct, dict):
            return {k: self._resolve_expressions(v, interpreter) for k, v in data_struct.items()}
        elif isinstance(data_struct, list):
            return [self._resolve_expressions(item, interpreter) for item in data_struct]
        elif isinstance(data_struct, str):
            # First try complete {{...}} match
            match = re.match(r"^\s*\{\{\s*(.+?)\s*\}\}\s*$", data_struct)
            if match:
                 expr = match.group(1).strip()
                 # --- Call imported evaluate (no parser needed) ---
                 return evaluate(expr, interpreter)
            
            # If no complete match, try to resolve embedded {{...}} expressions
            # This handles cases like ">= {{trigger.payload.amount}}"
            def replace_expr(match_obj):
                expr = match_obj.group(1).strip()
                result = evaluate(expr, interpreter)
                return str(result) if result is not None else match_obj.group(0)
            
            # Replace all {{...}} patterns in the string
            resolved = re.sub(r'\{\{\s*(.+?)\s*\}\}', replace_expr, data_struct)
            return resolved
        return data_struct

    def _evaluate_condition(self, value, condition_str):
        self.logger.debug(f"Evaluating condition: value='{value}' (type: {type(value)}), condition='{condition_str}'")
        if value is None:
             op_match = re.match(r"([><=!]+)\s*(.+)", condition_str.strip())
             if op_match:
                 op = op_match.group(1)
                 comp_val_str = op_match.group(2).strip()
                 if op == '==' and comp_val_str.lower() in ('null', 'none'): return True
                 if op == '!=' and comp_val_str.lower() not in ('null', 'none'): return True
             return False

        condition_str = str(condition_str).strip()
        simple_equality_match = re.match(r"^\s*([^><=!]+)\s*$", condition_str)
        if simple_equality_match:
            comp_val_str = simple_equality_match.group(1).strip()
            try:
                if isinstance(value, (int, float)):
                    comp_val = float(comp_val_str)
                    return value == comp_val
                elif isinstance(value, bool):
                     return value == (comp_val_str.lower() == 'true')
            except ValueError:
                 pass
            return str(value) == comp_val_str

        op_match = re.match(r"([><=!]+)\s*(.+)", condition_str)
        if not op_match:
             self.logger.warning(f"Could not parse condition: '{condition_str}'. Treating as string equality.")
             return str(value) == condition_str

        try:
            op, comp_val_str = op_match.groups()
            comp_val_str = comp_val_str.strip()

            if comp_val_str.lower() in ('null', 'none'):
                if op == '==': return value is None
                if op == '!=': return value is not None
                return False

            try:
                num_to_compare = float(comp_val_str)
                value_as_float = float(value)
                if op == '>':   return value_as_float > num_to_compare
                if op == '<':   return value_as_float < num_to_compare
                if op == '>=':  return value_as_float >= num_to_compare
                if op == '<=':  return value_as_float <= num_to_compare
                if op == '==':  return value_as_float == num_to_compare
                if op == '!=':  return value_as_float != num_to_compare
                self.logger.warning(f"Unknown numeric operator '{op}' in condition.")
                return False
            except (ValueError, TypeError):
                 if op == '==': return str(value) == comp_val_str
                 if op == '!=': return str(value) != comp_val_str
                 self.logger.warning(f"Cannot perform numeric comparison '{op}' on non-numeric value '{value}' or comparison value '{comp_val_str}'.")
                 return False
        except Exception as e:
            self.logger.error(f"Unexpected error evaluating condition '{condition_str}' against value '{value}': {e}")
            return False

    def _execute_logic_block(self, logic_block, context, correlation_id, owner_component_name):
        interpreter = ExpressionInterpreter(context)
        for write_op in logic_block.get('writes', []):
            try:
                 resolved_write = self._resolve_expressions(write_op['write'], interpreter)
                 table = resolved_write.get('table')
                 action = resolved_write.get('action')
                 data = resolved_write.get('values')
                 where = resolved_write.get('where')
                 if not table or not action or data is None:
                      self.logger.error(f"Invalid write operation parameters after resolution: {resolved_write}")
                      continue
                 self.write(table=table, action=action, data=data, where_clause=where,
                            correlation_id=correlation_id, owner_component=owner_component_name)
            except Exception as e:
                 self.logger.error(f"Failed to process or execute write operation: {write_op}. Error: {e}")

        for send_op in logic_block.get('sends', []):
            try:
                 resolved_send = self._resolve_expressions(send_op['send'], interpreter)
                 to = resolved_send.get('to')
                 message = resolved_send.get('message')
                 payload = resolved_send.get('payload', {})
                 if not to or not message:
                      self.logger.error(f"Invalid send operation parameters after resolution: {resolved_send}")
                      continue
                 
                 # Check for None values in payload
                 none_fields = [k for k, v in payload.items() if v is None]
                 if none_fields:
                     self.logger.warning(f"Payload for message '{message}' to '{to}' contains None values for fields: {none_fields}")
                 
                 self.send_message(to=to, message=message, payload=payload,
                                   correlation_id=correlation_id, owner_component=owner_component_name)
            except Exception as e:
                 self.logger.error(f"Failed to process or execute send operation: {send_op}. Error: {e}")

    def _execute_handler(self, handler, trigger_message, correlation_id):
        component_name = handler['component_name']
        self.logger.info(f"Executing handler '{handler['on_message']}' for component '{component_name}' [CID: {correlation_id}]")

        if self.mode == 'python' and component_name == self.target_component and self.py_handler_instance:
            try:
                method_name = f"on_{handler['on_message']}"
                handler_method = getattr(self.py_handler_instance, method_name)
                self.logger.info(f"[PYTHON] Calling {method_name} with payload: {trigger_message['payload']}")
                handler_method(trigger_message['payload'])
                return
            except Exception as e:
                self.logger.error(f"Error executing Python handler {method_name}: {e}", exc_info=True)
                raise

        # --- YAML MODE ---
        context = {'trigger': trigger_message, 'read': {}}
        interpreter = ExpressionInterpreter(context)

        logic = handler.get('logic', [])
        
        if not isinstance(logic, list):
            raise ValueError(f"Handler logic must be a list of steps. Found {type(logic).__name__} in handler '{handler.get('on_message')}'")
        
        # Execute modern Clotho logic (list of steps)
        self._execute_steps(logic, context, correlation_id, handler['component_name'])

    def _execute_steps(self, steps, context, correlation_id, component_name):
        interpreter = ExpressionInterpreter(context)
        for step in steps:
            if 'read' in step:
                table = step['read']
                key = step.get('key')
                where = step.get('where', {})
                if key:
                    where['id'] = key
                
                as_var = step.get('as')
                where = self._resolve_expressions(where, interpreter)
                
                read_result = self.read(table=table, where_clause=where, owner_component=component_name)
                context['read'][as_var] = read_result
            
            elif 'create' in step:
                table = step['create']
                data = step.get('data', {})
                data = self._resolve_expressions(data, interpreter)
                self.write(table=table, action='CREATE', data=data, correlation_id=correlation_id, owner_component=component_name)

            elif 'update' in step:
                table = step['update']
                where = step.get('where', {})
                set_data = step.get('set', {})
                
                where = self._resolve_expressions(where, interpreter)
                set_data = self._resolve_expressions(set_data, interpreter)
                
                self.write(table=table, action='UPDATE', data=set_data, where_clause=where, correlation_id=correlation_id, owner_component=component_name)
            
            elif 'send' in step:
                to = step['send'].get('to')
                message = step['send'].get('message')
                payload = step['send'].get('payload', {})
                payload = self._resolve_expressions(payload, interpreter)
                self.send_message(to=to, message=message, payload=payload, correlation_id=correlation_id, owner_component=component_name)

            elif 'match' in step:
                match_block = step['match']
                on_expression = match_block.get('on')
                match_value = self._resolve_expressions(on_expression, interpreter)
                
                executed_case = False
                for case in match_block.get('cases', []):
                    condition = case.get('when')
                    is_default = 'default' in case
                    case_matched = False
                    
                    if condition is not None:
                        resolved_condition = self._resolve_expressions(condition, interpreter)
                        if self._evaluate_condition(match_value, resolved_condition):
                            case_matched = True
                    elif is_default and not executed_case:
                        case_matched = True
                        
                    if case_matched:
                        # Execute nested steps
                        self._execute_steps(case.get('then', []), context, correlation_id, component_name)
                        executed_case = True
                        break

    def _find_owner_component(self, table_name):
        """
        Helper to find which component owns a given table.
        Uses Clotho v1.0 'state' structure (list of dicts).
        """
        for comp in self.clotho_data.get('components', []):
            # Clotho Standard: Check 'state' list
            if 'state' in comp:
                for state_item in comp['state']:
                    if state_item.get('name') == table_name:
                        return comp['name']
        return None

    def select_scenario(self, scenario_name):
        found = False
        for s in self.clotho_data.get('scenarios', []):
            if s and s.get('name') == scenario_name:
                 self.current_scenario = s
                 found = True
                 break
        if not found or not self.current_scenario:
             raise ValueError(f"Scenario '{scenario_name}' not found or is invalid.")

    def step(self):
        """Execute a single step of the simulation.
        
        Returns:
            bool: True if an event was processed, False if the queue is empty.
        """
        if not self.event_queue:
            return False

        # M3: Randomized scheduling - pick random event from queue
        if len(self.event_queue) > 1:
            index = self.rng.randint(0, len(self.event_queue) - 1)  # Use private RNG
            item = self.event_queue.pop(index)
        else:
            item = self.event_queue.pop(0)
        
        self.processed_event_count += 1
        
        if item['type'] == 'scenario_step':
            step = item['step']
            cid = item['cid']
            causation_id = item['causation_id']
            
            # Clotho v3 'send' syntax
            message = step.get('send')
            to_component = step.get('to')
            payload = step.get('payload', {})
            sender = step.get('from', '_UNKNOWN_')

            if not message or not to_component:
                    self.logger.warning(f"Skipping invalid step: {step}")
                    return True
            
            handler = self._find_handler(to_component, message)
            if handler:
                handler['component_name'] = to_component
                trigger = {
                        'sender': sender,
                        'message': message,
                        'payload': payload,
                        'timestamp': datetime.now(timezone.utc).isoformat() # Add timestamp to trigger context
                }
                
                # Log the start of handler execution
                event_id = self._generate_deterministic_id("evt", 12)
                self.logger.info(f"Event {event_id} | Handler: {message} | CID: {cid} | Parent: {causation_id or 'ROOT'}")
                
                # Log HANDLER_EXEC to DB
                ts = datetime.now(timezone.utc).isoformat()
                self.cursor.execute("""
                    INSERT INTO event_log 
                    (event_id, timestamp, correlation_id, causation_id, component, handler_name, 
                        trigger_message, table_name, action, payload, simulation_seed) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (event_id, ts, cid, causation_id, handler['component_name'], 
                        message, message, f"{handler['component_name']}_handler", 'HANDLER_EXEC', 
                        json.dumps(trigger.get('payload', {})), self.simulation_seed))
                
                # Generate logic tasks
                new_tasks = self._generate_handler_tasks(handler, trigger, cid, causation_id, event_id=event_id)
                self.event_queue.extend(new_tasks)
            else:
                self.logger.warning(f"No handler found for message '{message}' on component '{to_component}' [CID: {cid}]")

        elif item['type'] == 'handler_task':
            if not item['steps']:
                return True
            
            # Set context for execution
            self._current_event_id = item.get('event_id')
            self._current_causation_id = item.get('causation_id')
            
            step = item['steps'].pop(0)
            new_steps = self._execute_single_step(step, item['context'], item['cid'], item['component_name'])
            
            # If match step produced nested steps, prepend them to maintain local causality
            if new_steps:
                item['steps'] = new_steps + item['steps']
            
            # If steps remain, put the task back into the queue to be scheduled again
            if item['steps']:
                self.event_queue.append(item)
            else:
                # Handler finished
                # Commit transaction and check invariants
                self._current_cid = item['cid'] # Restore CID for invariant check logging
                self._current_event_id = item.get('causation_id') # Use causation ID as parent for invariant events? Or generate new?
                
                try:
                    self.conn.commit()
                    self._check_invariants()
                except RuntimeError as e:
                    # Re-raise invariant failures (which are RuntimeErrors in strict mode)
                    self.logger.error(f"Invariant Failure: {e}")
                    raise
                except Exception as e:
                    self.logger.error(f"Error during post-handler processing: {e}")

        return True

    def run(self):
        if not self.current_scenario: 
             # Auto-select if only one scenario exists
             scenarios = self.clotho_data.get('scenarios', [])
             if len(scenarios) == 1:
                 self.logger.info(f"Auto-selecting single scenario: {scenarios[0]['name']}")
                 self.current_scenario = scenarios[0]
             else:
                 raise RuntimeError("A scenario must be selected.")
        
        self._connect_db()
        start_time = datetime.now(timezone.utc).isoformat()
        try:
            self._create_database_schema()
            self._initialize_database_state()
            
            # M3: Record simulation metadata with seed
            scenario_name = self.current_scenario.get('name', 'UNKNOWN')
            self.cursor.execute("""
                INSERT INTO simulation_metadata 
                (simulation_seed, scenario_name, start_timestamp) 
                VALUES (?, ?, ?)
            """, (self.simulation_seed, scenario_name, start_time))
            self.conn.commit()
            
            # Event queue now contains: dicts representing tasks
            # Types: 'scenario_step', 'logic_step'
            scenario_cid = self._generate_deterministic_id("tx", 8)
            
            # Only support 'steps' (v3)
            raw_steps = self.current_scenario.get('steps', [])

            self.event_queue = []
            for step in raw_steps:
                if step and 'send' in step:
                    self.event_queue.append(
                        {'type': 'scenario_step', 'step': step, 'cid': scenario_cid, 'causation_id': None}
                    )
            
            self.processed_event_count = 0
            
            while self.step():
                if self.processed_event_count >= self.max_events:
                    self.logger.error(f"Maximum event limit ({self.max_events}) reached. Possible infinite loop detected.")
                    break
            
            # M3: Update simulation metadata with end time and event count
            end_time = datetime.now(timezone.utc).isoformat()
            self.cursor.execute("""
                UPDATE simulation_metadata 
                SET end_timestamp = ?, event_count = ?
                WHERE simulation_seed = ? AND start_timestamp = ?
            """, (end_time, self.processed_event_count, self.simulation_seed, start_time))
            self.conn.commit()
            
        except Exception as e:
            self.logger.critical(f"Unhandled exception during simulation run: {e}", exc_info=True)
            raise
        finally:
            if self.conn: self._close_db()
            self.logger.info(f"Simulation run finished. DB closed: {self.db_path}")

    def _read_full_table(self, component_name, table_name):
        full_table_name = f"{component_name}_{table_name}"
        try:
            self.cursor.execute(f"SELECT * FROM {full_table_name}")
            rows = self.cursor.fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error:
            return []

    def _check_invariants(self):
        # Check invariants for all components
        components = self.clotho_data.get('components', [])
        
        # Context for invariant checking - allows cross-component reads
        context = {
            'read': RootProxy(self)
        }
        
        interpreter = ExpressionInterpreter(context)
        strict_mode = self.config.get('strict_invariants', True) # Default to True for safety
        
        for comp in components:
            invariants = comp.get('invariants', [])
            for inv in invariants:
                inv_expr = inv
                inv_name = "Unnamed Invariant"
                
                # Support object-style invariants: { name: "...", expression: "..." }
                if isinstance(inv, dict):
                    inv_expr = inv.get('expression')
                    inv_name = inv.get('name', inv_expr)
                
                if not inv_expr: continue

                try:
                    result = evaluate(inv_expr, interpreter)
                    if result is False:
                        msg = f"Invariant FAILED in component '{comp.get('name', 'unknown')}': {inv_name} ({inv_expr})"
                        self.logger.error(msg)
                        
                        # Log failure to event log
                        ts = datetime.now(timezone.utc).isoformat()
                        fail_id = self._generate_deterministic_id("fail", 8)
                        # Ensure we have valid IDs for logging
                        cid = self._current_cid if hasattr(self, '_current_cid') else 'SYSTEM'
                        causation = self._current_event_id if hasattr(self, '_current_event_id') else 'SYSTEM'
                        
                        self.cursor.execute("""
                            INSERT INTO event_log 
                            (event_id, timestamp, correlation_id, causation_id, component, handler_name, 
                             trigger_message, table_name, action, payload, simulation_seed) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (fail_id, ts, cid, causation, comp.get('name', 'unknown'), 
                              'INVARIANT_CHECK', 'INVARIANT_FAILURE', 'SYSTEM', 'INVARIANT_FAIL', 
                              json.dumps({'error': msg, 'invariant': inv_expr}), self.simulation_seed))
                        self.conn.commit()
                        
                        if strict_mode:
                            raise RuntimeError(msg)
                            
                except Exception as e:
                    self.logger.error(f"Error evaluating invariant '{inv_expr}': {e}")
                    if strict_mode:
                        raise

    def execute_handler_wrapper(self, handler, trigger_message, correlation_id, causation_id=None):
        # Generate a unique event ID for this handler execution
        event_id = self._generate_deterministic_id("evt", 12)
        
        self._current_cid = correlation_id
        self._current_owner = handler['component_name']
        self._current_event_id = event_id
        self._current_causation_id = causation_id  # Store parent event ID
        
        # Log the event execution to event_log
        ts = datetime.now(timezone.utc).isoformat()
        handler_name = handler.get('on_message', 'UNKNOWN')
        trigger_msg_name = trigger_message.get('message', 'UNKNOWN')
        component_name = handler['component_name']
        
        self.logger.info(f"Event {event_id} | Handler: {handler_name} | CID: {correlation_id} | Parent: {causation_id or 'ROOT'}")
        
        # Log handler execution (even if it has no writes)
        # This ensures all events are tracked in the DAG
        self.cursor.execute("""
            INSERT INTO event_log 
            (event_id, timestamp, correlation_id, causation_id, component, handler_name, 
             trigger_message, table_name, action, payload, simulation_seed) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (event_id, ts, correlation_id, causation_id, component_name, 
              handler_name, trigger_msg_name, f"{component_name}_handler", 'HANDLER_EXEC', 
              json.dumps(trigger_message.get('payload', {})), self.simulation_seed))
        
        try:
            self._execute_handler(handler, trigger_message, correlation_id)
            self.conn.commit()
            self._check_invariants()
        except Exception as e:
            self.logger.error(f"Exception during handler execution for '{handler['on_message']}' [CID: {correlation_id}]: {e}")
            raise
        finally:
            self._current_cid = None
            self._current_owner = None
            self._current_event_id = None
            self._current_causation_id = None  # Clear parent event ID

    def read(self, table: str, where_clause: dict, columns: list = None, owner_component: str = None) -> dict:
        if columns is None: columns = ['*']
        if not owner_component: owner_component = self._current_owner
        if not owner_component:
             self.logger.error("Read called without owner component context.")
             return None
        owner_comp_name = self._find_owner_component(table)
        if not owner_comp_name:
             self.logger.warning(f"Could not find schema owner for table '{table}'. Assuming caller '{owner_component}' is the owner.")
             owner_comp_name = owner_component
        full_table_name = f"{owner_comp_name}_{table}"
        cols_str = ', '.join(columns)
        where_conditions = []
        where_values = []
        if where_clause:
             for k, v in where_clause.items():
                  if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', k):
                       where_conditions.append(f"{k} = ?")
                       where_values.append(v)
                  else:
                       self.logger.warning(f"Invalid key '{k}' in where_clause for read operation. Skipping.")
        where_str = " AND ".join(where_conditions)
        sql = f"SELECT {cols_str} FROM {full_table_name}" + (f" WHERE {where_str}" if where_str else "")
        try:
            self.cursor.execute(sql, tuple(where_values))
            result = self.cursor.fetchone()
            return dict(result) if result else None
        except sqlite3.Error as e:
            self.logger.error(f"READ failed for table {full_table_name}: {e}\nSQL: {sql}\nParams: {tuple(where_values)}")
            return None

    def write(self, table: str, action: str, data: dict, where_clause: dict = None, correlation_id: str = None, owner_component: str = None):
        ts = datetime.now(timezone.utc).isoformat()
        if not correlation_id: correlation_id = self._current_cid
        if not owner_component: owner_component = self._current_owner
        # Generate a unique event_id for this write operation (not reusing handler's event_id)
        event_id = self._generate_deterministic_id("evt", 12)
        causation_id = self._current_event_id  # Current handler event is the cause of this write
        
        if not correlation_id or not owner_component:
             self.logger.error("Write called without correlation_id or owner component context.")
             return
        table_owner = self._find_owner_component(table)
        if not table_owner:
             self.logger.warning(f"Could not find schema owner for table '{table}'. Assuming caller '{owner_component}' is the owner.")
             table_owner = owner_component
        full_table_name = f"{table_owner}_{table}"
        action_upper = action.upper()
        sql = None # Initialize sql
        params = None # Initialize params
        try:
            if action_upper == 'CREATE':
                if not data:
                    self.logger.warning("CREATE action called with empty data dict.")
                    return
                data_to_insert = data
                if not data_to_insert:
                     self.logger.warning("CREATE action resulted in empty data after filtering None.")
                     return
                cols = ', '.join(data_to_insert.keys())
                placeholders = ', '.join(['?'] * len(data_to_insert))
                sql = f"INSERT INTO {full_table_name} ({cols}) VALUES ({placeholders})"
                # Serialize dict/list values to JSON strings for SQLite
                params = tuple(json.dumps(v) if isinstance(v, (dict, list)) else v for v in data_to_insert.values())
                self.cursor.execute(sql, params)
                log_payload = data_to_insert
                self.cursor.execute("""
                    INSERT INTO event_log 
                    (event_id, timestamp, correlation_id, causation_id, component, handler_name, 
                     trigger_message, table_name, action, payload, simulation_seed) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (event_id, ts, correlation_id, causation_id, table_owner, 
                      owner_component, None, full_table_name, 'CREATE', json.dumps(log_payload), self.simulation_seed))
            elif action_upper == 'UPDATE':
                if not data or not where_clause:
                    self.logger.warning("UPDATE action called with empty data or where_clause dict.")
                    return
                set_data = {k: v for k, v in data.items() if k not in where_clause}
                where_data = {k: where_clause[k] for k in where_clause if k in where_clause}
                if not set_data or not where_data:
                     self.logger.warning("UPDATE action resulted in empty set_data or where_data after filtering.")
                     return
                set_clause = ', '.join([f"{k} = ?" for k in set_data.keys()])
                where_conditions = [f"{k} = ?" for k in where_data.keys()]
                where_clause_str = " AND ".join(where_conditions)
                if not where_clause_str:
                    self.logger.error("UPDATE action resulted in empty WHERE clause string.")
                    return
                sql = f"UPDATE {full_table_name} SET {set_clause} WHERE {where_clause_str}"
                # Serialize dict/list values to JSON strings for SQLite
                params = tuple(json.dumps(v) if isinstance(v, (dict, list)) else v for v in set_data.values()) + tuple(where_data.values())
                self.cursor.execute(sql, params)
                log_payload = {'update': set_data, 'where': where_data}
                self.cursor.execute("""
                    INSERT INTO event_log 
                    (event_id, timestamp, correlation_id, causation_id, component, handler_name, 
                     trigger_message, table_name, action, payload, simulation_seed) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (event_id, ts, correlation_id, causation_id, table_owner, 
                      owner_component, None, full_table_name, 'UPDATE', json.dumps(log_payload), self.simulation_seed))
            elif action_upper == 'DELETE':
                 if not where_clause:
                      self.logger.warning("DELETE action called without where_clause dict.")
                      return
                 where_conditions = [f"{k} = ?" for k in where_clause.keys()]
                 where_clause_str = " AND ".join(where_conditions)
                 if not where_clause_str:
                      self.logger.error("DELETE action resulted in empty WHERE clause string.")
                      return
                 sql = f"DELETE FROM {full_table_name} WHERE {where_clause_str}"
                 params = tuple(where_clause.values())
                 self.cursor.execute(sql, params)
                 log_payload = {'where': where_clause}
                 self.cursor.execute("""
                    INSERT INTO event_log 
                    (event_id, timestamp, correlation_id, causation_id, component, handler_name, 
                     trigger_message, table_name, action, payload, simulation_seed) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (event_id, ts, correlation_id, causation_id, table_owner, 
                      owner_component, None, full_table_name, 'DELETE', json.dumps(log_payload), self.simulation_seed))
            else:
                self.logger.error(f"Unsupported write action: {action}")
        except sqlite3.Error as e:
            self.logger.error(f"WRITE failed for action '{action}' on table {full_table_name}: {e}\nSQL: {sql if sql else 'N/A'}\nParams: {params if params else 'N/A'}")
            raise

    def send_message(self, to: str, message: str, payload: dict, correlation_id: str = None, owner_component: str = None):
        if not correlation_id: correlation_id = self._current_cid
        if not owner_component: owner_component = self._current_owner
        parent_event_id = self._current_event_id  # Current event becomes the parent (causation_id) of the new event
        
        if not correlation_id or not owner_component:
             self.logger.error("Send message called without correlation_id or owner component context.")
             return
        if not to or not message:
             self.logger.error(f"Send message called with invalid 'to' ('{to}') or 'message' ('{message}') fields.")
             return
        if not isinstance(payload, dict):
             self.logger.warning(f"Payload for message '{message}' to '{to}' is not a dictionary (type: {type(payload)}). Sending empty payload instead.")
             payload = {}
        
        # --- FAULT INJECTION (M4) ---
        # Check for MessageDrop faults
        run_config = self.clotho_data.get('run', {})
        env_config = run_config.get('environment', {})
        faults = env_config.get('faults', [])
        
        for fault in faults:
            if fault.get('type') == 'MessageDrop':
                target = fault.get('target')
                prob = fault.get('probability', 0.0)
                
                # Check if this message matches the fault criteria
                # Currently only filtering by target component
                if target == to or target == '*':
                    if self.rng.random() < prob:
                        self.logger.info(f"FAULT INJECTION: MessageDrop triggered! Dropping message '{message}' to '{to}'")
                        
                        # Log fault injection
                        ts = datetime.now(timezone.utc).isoformat()
                        event_id = self._generate_deterministic_id("fault", 12)
                        self.cursor.execute("""
                            INSERT INTO event_log 
                            (event_id, timestamp, correlation_id, causation_id, component, handler_name, 
                             trigger_message, table_name, action, payload, simulation_seed) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (event_id, ts, correlation_id, parent_event_id, owner_component, 
                              None, message, 'FAULT', 'FAULT_INJECTION', 
                              json.dumps({'fault_type': 'MessageDrop', 'target': to}), self.simulation_seed))
                        return # Drop the message (don't add to queue)

        new_step = {
            'send': message,
            'to': to,
            'payload': payload,
            'from': owner_component
        }
        
        # Event queue now contains: dicts representing tasks
        self.event_queue.append({
            'type': 'scenario_step', 
            'step': new_step, 
            'cid': correlation_id, 
            'causation_id': self._current_event_id # Use current event ID as causation
        })
        self.logger.debug(f"Queued message: '{message}' from '{owner_component}' to '{to}' [CID: {correlation_id}] [Parent: {self._current_event_id}]")

    def _find_handler(self, component_name, message_name):
        for comp in self.clotho_data.get('components', []):
            if comp and comp.get('name') == component_name:
                for h in comp.get('handlers', []):
                     if h and h.get('on_message') == message_name:
                          return h
        return None
    
    def get_all_states(self) -> dict:
        """
        M5: Get all current component states for fingerprinting.
        
        Returns:
            Dict mapping table_name -> list of rows (as dicts)
            Example: {
                'AccountService_account': [{'id': 'alice', 'balance': 1000}, ...],
                'TransactionService_transaction': [...]
            }
        """
        if not self.conn:
            return {}
        
        states = {}
        
        # Get all table names
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'event_log%' AND name NOT LIKE 'simulation_metadata%'")
        tables = self.cursor.fetchall()
        
        for (table_name,) in tables:
            # Query all rows from this table
            try:
                self.cursor.execute(f"SELECT * FROM {table_name}")
                rows = self.cursor.fetchall()
                
                # Convert Row objects to dicts
                states[table_name] = [dict(row) for row in rows]
            except sqlite3.Error as e:
                self.logger.warning(f"Failed to read state from table {table_name}: {e}")
                states[table_name] = []
        
        return states

    def _synthesize_scenarios_from_run(self, run_config):
        """
        Converts Clotho 'run' section (fixtures + generators) into a 'scenario'.
        This allows the existing simulator loop to execute Clotho definitions.
        """
        scenario = {
            'name': 'Auto-Generated Simulation',
            'description': 'Synthesized from Clotho run configuration',
            'initial_state': [],
            'steps': []
        }

        # 1. Convert Fixtures to Initial State
        fixtures = run_config.get('fixtures', [])
        # Group by component
        comp_fixtures = {}
        for f in fixtures:
            c_name = f['component']
            if c_name not in comp_fixtures:
                comp_fixtures[c_name] = {}
            
            t_name = f['table']
            if t_name not in comp_fixtures[c_name]:
                comp_fixtures[c_name][t_name] = []
            
            comp_fixtures[c_name][t_name].extend(f.get('rows', []))
        
        for c_name, state_data in comp_fixtures.items():
            scenario['initial_state'].append({
                'component': c_name,
                'state': state_data
            })

        # 2. Convert Generators to Sequence
        # NOTE: This is a naive deterministic expansion. 
        # A real Fuzzer would do this dynamically.
        generators = run_config.get('generators', [])
        for gen in generators:
            count = gen.get('count', 1)
            behavior = gen.get('behavior', {})
            
            # If behavior is simple 'send', expand it
            if 'send' in behavior:
                msg_type = behavior['send']
                fuzz_hint = behavior.get('fuzz_hint', {})
                
                for i in range(count):
                    # Construct payload from fuzz_hint (naive static values for now)
                    payload = {}
                    for k, v in fuzz_hint.items():
                        if isinstance(v, dict) and 'range' in v:
                            # Pick middle of range for deterministic simple run
                            r = v['range']
                            payload[k] = (r[0] + r[1]) // 2
                        else:
                            payload[k] = v
                    
                    # We need to know 'to' and 'from' from the message definition
                    # But here we just put what we have. 
                    # The example has 'from_id' and 'to_id' in fuzz_hint.
                    
                    # 'send' implies an action.
                    # We map it to the old 'action' format.
                    # We assume the target component is implied or we need to look it up.
                    # Actually, the old format requires 'to'.
                    # 'InitiateTransfer' is handled by 'AccountService'.
                    # We need to find which component handles this message.
                    target_component = self._find_handler_for_message(msg_type)
                    
                    # Also check if payload is defined in 'payload' key (not just fuzz_hint)
                    if 'payload' in behavior:
                        import copy
                        static_payload = copy.deepcopy(behavior['payload'])
                        
                        # Replace $sequence
                        for k, v in static_payload.items():
                            if v == "$sequence":
                                static_payload[k] = i
                        
                        payload.update(static_payload)

                    scenario['steps'].append({
                        'send': msg_type,
                        'to': target_component or 'Unknown',
                        'payload': payload
                    })

        return [scenario]

    def _find_handler_for_message(self, msg_type):
        """Helper to find which component handles a given message type."""
        for comp in self.clotho_data.get('components', []):
            for handler in comp.get('handlers', []):
                # Clotho uses 'on_message', Legacy uses 'on'
                if handler.get('on') == msg_type or handler.get('on_message') == msg_type:
                    return comp['name']
        return None

    def _generate_handler_tasks(self, handler, trigger_message, correlation_id, causation_id, event_id=None):
        component_name = handler['component_name']
        
        # Python mode check (omitted for brevity, assuming YAML for now or handled elsewhere)
        if self.mode == 'python' and component_name == self.target_component and self.py_handler_instance:
             # For Python mode, we can't easily break it down, so we execute it immediately
             # This breaks interleaving for Python handlers, but that's expected
             self.execute_handler_wrapper(handler, trigger_message, correlation_id, causation_id)
             return []

        context = {'trigger': trigger_message, 'read': {}}
        logic = handler.get('logic', [])
        
        # Create a single task representing the handler execution flow
        # The scheduler will pick this task, execute one step, and put it back if more steps remain
        task = {
            'type': 'handler_task',
            'steps': list(logic),
            'context': context,
            'component_name': component_name,
            'cid': correlation_id,
            'causation_id': causation_id,
            'event_id': event_id
        }
            
        return [task]

    def _execute_single_step(self, step, context, correlation_id, component_name):
        interpreter = ExpressionInterpreter(context)
        new_steps = []
        
        if 'read' in step:
            table = step['read']
            key = step.get('key')
            where = step.get('where', {})
            if key:
                where['id'] = key
            
            as_var = step.get('as')
            where = self._resolve_expressions(where, interpreter)
            
            read_result = self.read(table=table, where_clause=where, owner_component=component_name)
            context['read'][as_var] = read_result
        
        elif 'create' in step:
            table = step['create']
            data = step.get('data', {})
            data = self._resolve_expressions(data, interpreter)
            self.write(table=table, action='CREATE', data=data, correlation_id=correlation_id, owner_component=component_name)

        elif 'update' in step:
            table = step['update']
            where = step.get('where', {})
            set_data = step.get('set', {})
            
            where = self._resolve_expressions(where, interpreter)
            set_data = self._resolve_expressions(set_data, interpreter)
            
            self.write(table=table, action='UPDATE', data=set_data, where_clause=where, correlation_id=correlation_id, owner_component=component_name)
        
        elif 'send' in step:
            to = step['send'].get('to')
            message = step['send'].get('message')
            payload = step['send'].get('payload', {})
            payload = self._resolve_expressions(payload, interpreter)
            self.send_message(to=to, message=message, payload=payload, correlation_id=correlation_id, owner_component=component_name)

        elif 'match' in step:
            match_block = step['match']
            on_expression = match_block.get('on')
            match_value = self._resolve_expressions(on_expression, interpreter)
            
            executed_case = False
            for case in match_block.get('cases', []):
                condition = case.get('when')
                is_default = 'default' in case
                case_matched = False
                
                if condition is not None:
                    resolved_condition = self._resolve_expressions(condition, interpreter)
                    if self._evaluate_condition(match_value, resolved_condition):
                        case_matched = True
                elif is_default and not executed_case:
                    case_matched = True
                    
                if case_matched:
                    # Return nested steps to be prepended to the task's step list
                    new_steps = case.get('then', [])
                    executed_case = True
                    break
            
        return new_steps

    # --- Verification Engine (LTL) ---
    def verify_invariants(self):
        """
        Checks all invariants defined in the 'test' section against the event log.
        Returns a dict {invariant_name: bool (passed/failed)}.
        """
        results = {}
        invariants = self.clotho_data.get('test', {}).get('invariants', [])
        
        if not invariants:
            return results

        # Ensure DB is connected
        if self.conn is None:
            self._connect_db(reset=False)

        # Fetch all events
        self.cursor.execute("SELECT * FROM event_log ORDER BY id ASC")
        events = [dict(row) for row in self.cursor.fetchall()]
        
        for inv in invariants:
            name = inv.get('name', 'Unnamed')
            check_str = inv.get('check', '')
            
            # Parse "always(A -> eventually(B))"
            match = re.match(r"always\((.*)\s*->\s*eventually\((.*)\)\)", check_str)
            if match:
                condition_a = match.group(1).strip()
                condition_b = match.group(2).strip()
                
                passed = self._check_ltl_always_eventually(events, condition_a, condition_b)
                results[name] = passed
            else:
                self.logger.warning(f"Unsupported invariant syntax: {check_str}")
                results[name] = False # Fail unsupported for now
                
        return results

    def _check_ltl_always_eventually(self, events, condition_a, condition_b):
        """
        Verifies: For every event matching A, there is a subsequent event matching B.
        """
        pending_obligations = [] # List of {event_index, context}
        
        for i, event in enumerate(events):
            # Check if this event triggers an obligation (A)
            if self._event_matches(event, condition_a):
                # Store the obligation. We might need context (like ID) to match B.
                # For now, we assume B must match "some" event later.
                # But usually it's "eventually(B(id))".
                # Let's extract variables from A's match to constrain B.
                context = self._extract_match_context(event, condition_a)
                pending_obligations.append({'index': i, 'context': context})
            
            # Check if this event satisfies any pending obligations (B)
            # We iterate backwards to allow removing items safely or use a new list
            remaining_obligations = []
            for obligation in pending_obligations:
                if self._event_matches(event, condition_b, obligation['context']):
                    # Obligation satisfied!
                    pass 
                else:
                    remaining_obligations.append(obligation)
            pending_obligations = remaining_obligations
            
        if pending_obligations:
            self.logger.error(f"Verification Failed! {len(pending_obligations)} pending obligations remaining.")
            return False
        return True

    def _event_matches(self, event, condition_str, context=None):
        """
        Checks if an event matches a condition string.
        Supported syntax:
        - msg.MessageName
        - read.Component.Table.Field == 'Value'
        """
        # 1. Message Match: msg.StartTask
        if condition_str.startswith("msg."):
            msg_name = condition_str.split(".")[1]
            return event.get('trigger_message') == msg_name
        
        # 2. State Change Match: read.Process.tasks.status == 'COMPLETED'
        # In event log, this looks like: component='Process', table_name='tasks', payload={'status': 'COMPLETED', ...}
        if condition_str.startswith("read."):
            # Parse: read.Component.Table.Field == 'Value'
            # Regex to split: read\.(\w+)\.(\w+)\.(\w+)\s*==\s*(['"]?[\w]+['"]?)
            match = re.match(r"read\.(\w+)\.(\w+)\.(\w+)\s*==\s*(.+)", condition_str)
            if match:
                comp = match.group(1)
                table = match.group(2)
                field = match.group(3)
                value_str = match.group(4).strip("'\"")
                
                expected_table_name = f"{comp}_{table}"
                if event['component'] == comp and (event['table_name'] == table or event['table_name'] == expected_table_name):
                    try:
                        payload = json.loads(event['payload'])
                        # Handle UPDATE payload structure
                        if 'update' in payload and isinstance(payload['update'], dict):
                             payload = payload['update']
                        
                        if field in payload:
                            # Check value equality
                            if str(payload[field]) == value_str:
                                return True
                    except:
                        pass
        
        return False

    def _extract_match_context(self, event, condition_str):
        """
        Extracts context from a match to constrain future matches.
        For now, returns empty dict as we don't support variable binding in LTL yet.
        """
        return {}
