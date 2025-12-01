"""
core/fuzzer.py

M6: Statistical Fuzzing Engine
Automatically randomize inputs, states, and scenarios to find edge cases

Fuzzing Types:
1. Input Fuzzing: Randomize message payloads
2. State Fuzzing: Randomize initial database states
3. Scenario Fuzzing: Combine scenarios in unexpected ways
"""

import random
import copy
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class FuzzingConfig:
    """Configuration for fuzzing behavior"""
    fuzz_inputs: bool = True
    fuzz_states: bool = True
    fuzz_scenarios: bool = False
    boundary_value_prob: float = 0.3  # Probability of using boundary values
    type_confusion_prob: float = 0.2  # Probability of type confusion
    null_prob: float = 0.1  # Probability of null/None values
    extreme_value_prob: float = 0.2  # Probability of extreme values
    seed: Optional[int] = None


class InputFuzzer:
    """
    M6: Fuzz message payloads to find edge cases
    
    Strategies:
    - Boundary values: 0, -1, MAX_INT, empty strings
    - Type confusion: String→Number, Number→String
    - Null injection: None, null, empty
    - Extreme values: Very large/small numbers, long strings
    """
    
    def __init__(self, config: FuzzingConfig):
        self.config = config
        # CRITICAL FIX: Use private Random instance to avoid thread pollution
        seed = config.seed if config.seed is not None else self.rng.randint(0, 2**32 - 1)
        self.rng = random.Random(seed)
    
    def fuzz_payload(self, payload: Dict[str, Any], schema: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Fuzz a message payload
        
        Args:
            payload: Original payload dict
            schema: Optional schema for smarter fuzzing
            
        Returns:
            Fuzzed payload
        """
        if not self.config.fuzz_inputs:
            return payload
        
        fuzzed = copy.deepcopy(payload)
        
        for key, value in fuzzed.items():
            # Decide whether to fuzz this field
            if self.rng.random() < 0.5:  # 50% chance to fuzz each field
                fuzzed[key] = self._fuzz_value(value, key)
        
        return fuzzed
    
    def _fuzz_value(self, value: Any, key: str) -> Any:
        """Fuzz a single value"""
        
        # Null injection
        if self.rng.random() < self.config.null_prob:
            return None
        
        # Type-based fuzzing (bool must be checked before int!)
        if isinstance(value, bool):
            return self._fuzz_boolean(value)
        elif isinstance(value, int):
            return self._fuzz_integer(value, key)
        elif isinstance(value, float):
            return self._fuzz_float(value)
        elif isinstance(value, str):
            return self._fuzz_string(value, key)
        elif isinstance(value, list):
            return self._fuzz_list(value)
        elif isinstance(value, dict):
            return self.fuzz_payload(value)
        
        return value
    
    def _fuzz_integer(self, value: int, key: str) -> Any:
        """Fuzz integer values"""
        
        # Boundary values
        if self.rng.random() < self.config.boundary_value_prob:
            return self.rng.choice([
                0,           # Zero
                -1,          # Negative boundary
                1,           # Positive boundary
                2**31 - 1,   # MAX_INT (32-bit)
                -2**31,      # MIN_INT (32-bit)
                2**63 - 1,   # MAX_LONG (64-bit)
            ])
        
        # Type confusion
        if self.rng.random() < self.config.type_confusion_prob:
            return str(value)  # Int → String
        
        # Extreme values
        if self.rng.random() < self.config.extreme_value_prob:
            return self.rng.choice([
                value * 1000000,  # Very large
                value * -1,       # Negated
                abs(value),       # Absolute
            ])
        
        # Small random perturbation
        return value + self.rng.randint(-10, 10)
    
    def _fuzz_float(self, value: float) -> Any:
        """Fuzz float values"""
        
        # Boundary values
        if self.rng.random() < self.config.boundary_value_prob:
            return self.rng.choice([
                0.0,
                -0.0,
                float('inf'),
                float('-inf'),
                # float('nan'),  # NaN can cause issues, commented out
            ])
        
        # Type confusion
        if self.rng.random() < self.config.type_confusion_prob:
            return int(value)  # Float → Int
        
        # Small perturbation
        return value * self.rng.uniform(0.5, 2.0)
    
    def _fuzz_string(self, value: str, key: str) -> Any:
        """Fuzz string values"""
        
        # Boundary values
        if self.rng.random() < self.config.boundary_value_prob:
            return self.rng.choice([
                "",              # Empty string
                " ",             # Whitespace
                "\n\t",          # Special chars
                "NULL",          # SQL injection attempt
                "0",             # Numeric string
                "true",          # Boolean string
            ])
        
        # Type confusion (String → Number)
        if self.rng.random() < self.config.type_confusion_prob:
            try:
                return int(value)
            except ValueError:
                pass
        
        # Extreme length
        if self.rng.random() < self.config.extreme_value_prob:
            return value * 100  # Very long string
        
        # Random mutation
        if len(value) > 0:
            idx = self.rng.randint(0, len(value) - 1)
            chars = list(value)
            chars[idx] = chr(self.rng.randint(32, 126))
            return ''.join(chars)
        
        return value
    
    def _fuzz_boolean(self, value: bool) -> Any:
        """Fuzz boolean values"""
        
        # Type confusion
        if self.rng.random() < self.config.type_confusion_prob:
            return self.rng.choice([
                1 if value else 0,    # Bool → Int
                "true" if value else "false",  # Bool → String
            ])
        
        # Flip
        return not value
    
    def _fuzz_list(self, value: List) -> List:
        """Fuzz list values"""
        
        # Empty list
        if self.rng.random() < self.config.boundary_value_prob:
            return []
        
        # Very large list
        if self.rng.random() < self.config.extreme_value_prob:
            return value * 100
        
        # Fuzz each element
        return [self._fuzz_value(v, "") for v in value]


class StateFuzzer:
    """
    M6: Fuzz initial database states
    
    Strategies:
    - Random balances/quantities
    - Random record counts (0, 1, 1000+)
    - Empty tables
    - Large datasets
    """
    
    def __init__(self, config: FuzzingConfig):
        self.config = config
        # CRITICAL FIX: Use private Random instance
        seed = config.seed if config.seed is not None else random.randint(0, 2**32 - 1)
        self.rng = random.Random(seed)
    
    def fuzz_initial_state(self, initial_state: List[Dict]) -> List[Dict]:
        """
        Fuzz initial database state
        
        Args:
            initial_state: Original initial_state from scenario
            
        Returns:
            Fuzzed initial state
        """
        if not self.config.fuzz_states:
            return initial_state
        
        fuzzed = copy.deepcopy(initial_state)
        
        for component_state in fuzzed:
            state_data = component_state.get('state', {})
            
            for table_name, records in state_data.items():
                # Empty table
                if self.rng.random() < 0.2:
                    state_data[table_name] = []
                    continue
                
                # Large dataset
                if self.rng.random() < 0.2:
                    # Duplicate records with variation
                    expanded = []
                    for _ in range(self.rng.randint(10, 100)):
                        for record in records:
                            fuzzed_record = self._fuzz_record(record)
                            expanded.append(fuzzed_record)
                    state_data[table_name] = expanded
                    continue
                
                # Fuzz each record
                state_data[table_name] = [self._fuzz_record(r) for r in records]
        
        return fuzzed
    
    def _fuzz_record(self, record: Dict) -> Dict:
        """Fuzz a single database record"""
        fuzzed = copy.deepcopy(record)
        
        for key, value in fuzzed.items():
            # Don't fuzz IDs (causes foreign key issues)
            if 'id' in key.lower():
                continue
            
            # Fuzz numeric fields (balances, quantities, etc.)
            if isinstance(value, (int, float)):
                fuzzed[key] = self._fuzz_numeric_field(value)
            
            # Fuzz string fields
            elif isinstance(value, str):
                fuzzed[key] = self._fuzz_string_field(value)
        
        return fuzzed
    
    def _fuzz_numeric_field(self, value: float) -> float:
        """Fuzz numeric fields (balances, quantities)"""
        
        # Boundary values
        if self.rng.random() < self.config.boundary_value_prob:
            return self.rng.choice([0, -1, 1, 1000000])
        
        # Random scale
        return value * self.rng.uniform(0.1, 10.0)
    
    def _fuzz_string_field(self, value: str) -> str:
        """Fuzz string fields"""
        
        # Empty
        if self.rng.random() < 0.1:
            return ""
        
        # Random string
        if self.rng.random() < 0.1:
            return ''.join(self.rng.choices('abcdefghijklmnopqrstuvwxyz', k=self.rng.randint(5, 15)))
        
        return value


class ScenarioFuzzer:
    """
    M6: Combine scenarios in unexpected ways
    
    Strategies:
    - Sequential chaining: Scenario A → Scenario B
    - Parallel execution: Run A and B concurrently
    - Random interleaving: Mix events from multiple scenarios
    """
    
    def __init__(self, config: FuzzingConfig):
        self.config = config
        # CRITICAL FIX: Use private Random instance
        seed = config.seed if config.seed is not None else random.randint(0, 2**32 - 1)
        self.rng = random.Random(seed)
    
    def chain_scenarios(self, scenarios: List[Dict], mode: str = 'sequential') -> Dict:
        """
        Chain multiple scenarios together
        
        Args:
            scenarios: List of scenario dicts
            mode: 'sequential', 'parallel', or 'interleaved'
            
        Returns:
            Combined mega-scenario
        """
        if not self.config.fuzz_scenarios or len(scenarios) < 2:
            return scenarios[0] if scenarios else {}
        
        if mode == 'sequential':
            return self._chain_sequential(scenarios)
        elif mode == 'parallel':
            return self._chain_parallel(scenarios)
        elif mode == 'interleaved':
            return self._chain_interleaved(scenarios)
        
        return scenarios[0]
    
    def _chain_sequential(self, scenarios: List[Dict]) -> Dict:
        """Chain scenarios sequentially (A then B then C)"""
        combined = {
            'name': 'fuzzed_sequential_' + '_'.join([s.get('name', 'unknown') for s in scenarios]),
            'initial_state': scenarios[0].get('initial_state', []),
            'steps': []
        }
        
        # Concatenate all sequences
        for scenario in scenarios:
            combined['steps'].extend(scenario.get('steps', []))
        
        return combined
    
    def _chain_parallel(self, scenarios: List[Dict]) -> Dict:
        """Run scenarios in parallel (interleave all events)"""
        combined = {
            'name': 'fuzzed_parallel_' + '_'.join([s.get('name', 'unknown') for s in scenarios]),
            'initial_state': scenarios[0].get('initial_state', []),
            'steps': []
        }
        
        # Collect all events
        all_events = []
        for scenario in scenarios:
            all_events.extend(scenario.get('steps', []))
        
        # Shuffle for random parallel execution
        self.rng.shuffle(all_events)
        combined['steps'] = all_events
        
        return combined
    
    def _chain_interleaved(self, scenarios: List[Dict]) -> Dict:
        """Interleave scenarios (round-robin from each)"""
        combined = {
            'name': 'fuzzed_interleaved_' + '_'.join([s.get('name', 'unknown') for s in scenarios]),
            'initial_state': scenarios[0].get('initial_state', []),
            'steps': []
        }
        
        # Get sequences
        sequences = [s.get('steps', []) for s in scenarios]
        
        # Interleave round-robin
        max_len = max(len(seq) for seq in sequences)
        for i in range(max_len):
            for seq in sequences:
                if i < len(seq):
                    combined['steps'].append(seq[i])
        
        return combined
