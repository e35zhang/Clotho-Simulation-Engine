"""
M4: Chaos Matrix - Parallel Simulation Engine
M5: Enhanced with State Coverage Tracking and Reliability Scoring
M6: Enhanced with Statistical Fuzzing

Run thousands of simulations with different seeds to find rare race conditions.

Key Features:
- Batch execution with configurable parallelism
- Progress tracking with callbacks
- Automatic failure detection and seed capture
- Statistical analysis of results
- M5: State fingerprinting and reliability scoring
- M6: Input/State/Scenario fuzzing
"""

import os
import time
import random
import sqlite3
import concurrent.futures
from typing import Dict, List, Callable, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime

from core.engine.clotho_simulator import Simulator
from .coverage_tracker import CoverageTracker, compute_state_fingerprint
from .reliability_scorer import ReliabilityScorer
from .fuzzer import FuzzingConfig, InputFuzzer, StateFuzzer, ScenarioFuzzer


@dataclass
class SimulationResult:
    """Result from a single simulation run"""
    seed: int
    success: bool
    event_count: int
    execution_time_ms: float
    db_path: str
    error_message: Optional[str] = None
    final_state: Optional[Dict[str, Any]] = None  # Can store final DB state for comparison
    state_fingerprints: List[str] = field(default_factory=list)  # M5: Collected state hashes


@dataclass
class ChaosMatrixStats:
    """Aggregated statistics from Chaos Matrix run"""
    total_runs: int
    completed: int
    failed: int
    success_rate: float
    total_execution_time_ms: float
    avg_execution_time_ms: float
    min_execution_time_ms: float
    max_execution_time_ms: float
    failing_seeds: List[int] = field(default_factory=list)
    unique_failure_patterns: int = 0
    failure_messages: List[str] = field(default_factory=list)
    # M5: Reliability scoring fields
    reliability_score: Optional[float] = None
    unique_states: Optional[int] = None
    total_state_observations: Optional[int] = None
    state_coverage_rate: Optional[float] = None


class ChaosMatrix:
    """
    M4: Parallel Chaos Matrix Engine
    M5: Enhanced with state coverage tracking
    M6: Enhanced with statistical fuzzing
    
    Runs multiple simulations with different seeds to explore state space.
    """
    
    def __init__(self, clotho_data: dict, scenario_name: str, max_workers: Optional[int] = None, 
                 track_coverage: bool = True, fuzzing_config: Optional[FuzzingConfig] = None):
        """
        Initialize Chaos Matrix
        
        Args:
            clotho_data: Parsed Clotho YAML specification
            scenario_name: Scenario to run
            max_workers: Max parallel workers (default: CPU count)
            track_coverage: Enable state coverage tracking (M5 feature)
            fuzzing_config: Fuzzing configuration (M6 feature)
        """
        self.clotho_data = clotho_data
        self.scenario_name = scenario_name
        self.max_workers = max_workers or os.cpu_count()
        self.track_coverage = track_coverage
        self.coverage_tracker = CoverageTracker() if track_coverage else None
        
        # M6: Fuzzing
        self.fuzzing_config = fuzzing_config or FuzzingConfig(fuzz_inputs=False, fuzz_states=False, fuzz_scenarios=False)
        # CRITICAL FIX: Don't create shared Fuzzer instances here
        # Each worker thread will create its own instances in _apply_fuzzing()
        
    def _run_single_simulation(self, seed: int) -> SimulationResult:
        """
        Run a single simulation with given seed (M6: applies fuzzing)
        
        Args:
            seed: Simulation seed
            
        Returns:
            SimulationResult with execution details
        """
        start_time = time.time()
        state_fingerprints = []
        
        try:
            # M6: Apply fuzzing to Clotho data for this specific run
            fuzzed_clotho_data = self._apply_fuzzing(seed)
            
            # Create simulator with specific seed
            sim = Simulator(clotho_data=fuzzed_clotho_data, simulation_seed=seed)
            sim.select_scenario(self.scenario_name)
            
            # M5: Collect state fingerprints during simulation if tracking enabled
            if self.track_coverage:
                # Capture initial state
                initial_states = sim.get_all_states()
                initial_fp = compute_state_fingerprint(initial_states)
                state_fingerprints.append(initial_fp)
            
            # Run simulation
            sim.run()
            
            # NOTE: Cannot call sim.get_all_states() here - DB is closed after run() completes
            # The final state was captured at the last checkpoint during simulation
            
            execution_time = (time.time() - start_time) * 1000
            
            # After simulation completes, database is closed
            # Reopen to count events and validate final state
            conn = sqlite3.connect(sim.db_path)
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM event_log")
            event_count = cursor.fetchone()[0]

            # M5: Capture final state fingerprint
            if self.track_coverage:
                try:
                    final_states = {}
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'event_log%' AND name NOT LIKE 'simulation_metadata%'")
                    tables = cursor.fetchall()
                    for (table_name,) in tables:
                        cursor.execute(f"SELECT * FROM {table_name}")
                        # Convert rows to dicts (simple approximation for fingerprinting)
                        # We don't need full dicts, just consistent representation
                        rows = cursor.fetchall()
                        # Sort rows to ensure deterministic fingerprinting regardless of insertion order
                        # (unless order matters, but for state set it usually doesn't)
                        final_states[table_name] = sorted([str(row) for row in rows])
                    
                    final_fp = compute_state_fingerprint(final_states)
                    state_fingerprints.append(final_fp)
                except Exception as e:
                    # Don't fail the run if fingerprinting fails
                    print(f"Warning: Failed to capture final state fingerprint: {e}")
            
            # VERIFY INVARIANTS from YAML test section
            validation_error = None
            invariants = fuzzed_clotho_data.get('test', {}).get('invariants', [])
            if invariants:
                # Fetch all events for invariant checking
                cursor.execute("SELECT * FROM event_log ORDER BY id ASC")
                events = [dict(row) for row in cursor.fetchall()]
                
                # Get final state for state-based checks
                final_state = {}
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'event_log%' AND name NOT LIKE 'simulation_metadata%'")
                for (table_name,) in cursor.fetchall():
                    cursor.execute(f"SELECT * FROM {table_name}")
                    final_state[table_name] = [dict(row) for row in cursor.fetchall()]
                
                for inv in invariants:
                    inv_name = inv.get('name', 'Unnamed')
                    check_str = inv.get('check', '')
                    
                    # Custom check: score_matches_action_count
                    if check_str == 'score_matches_action_count':
                        # Count PlayerAction events for this scenario
                        action_count = sum(1 for e in events if e.get('trigger_message') == 'PlayerAction')
                        expected_score = action_count * 10
                        
                        # Find the player_id used in this scenario
                        scenario = next((s for s in fuzzed_clotho_data.get('run', {}).get('scenarios', []) 
                                        if s.get('name') == self.scenario_name), None)
                        target_player_id = None
                        if scenario:
                            for step in scenario.get('steps', []):
                                payload = step.get('payload', {})
                                if 'player_id' in payload:
                                    target_player_id = payload['player_id']
                                    break
                        
                        # Check score only for the target player
                        found_player = False
                        for table_name, rows in final_state.items():
                            for row in rows:
                                if 'score' in row and 'player_id' in row:
                                    # Only check the player from this scenario
                                    if target_player_id and row.get('player_id') != target_player_id:
                                        continue
                                    found_player = True
                                    actual_score = row['score']
                                    # Skip None (player not created yet - timing issue, not race condition)
                                    if actual_score is None:
                                        continue
                                    # Convert to int for comparison (DB might store as string)
                                    try:
                                        actual_score = int(actual_score)
                                    except (ValueError, TypeError):
                                        pass
                                    if actual_score != expected_score:
                                        validation_error = f"RACE CONDITION: Invariant '{inv_name}' FAILED! score={actual_score}, expected={expected_score} ({action_count} PlayerAction events)"
                                        break
                            if validation_error:
                                break
                        if validation_error:
                            break
                    
                    # Custom check: total_balance_conserved (for banking demos)
                    elif check_str == 'total_balance_conserved':
                        # Get initial total from scenario
                        scenario = next((s for s in fuzzed_clotho_data.get('run', {}).get('scenarios', []) 
                                        if s.get('name') == self.scenario_name), None)
                        initial_total = 0.0
                        if scenario and 'initial_state' in scenario:
                            for init in scenario['initial_state']:
                                storage = init.get('storage', {})
                                for table_name, rows in storage.items():
                                    for row in rows:
                                        if 'balance' in row:
                                            initial_total += float(row['balance'])
                        
                        # Calculate final total
                        final_total = 0.0
                        for table_name, rows in final_state.items():
                            for row in rows:
                                if 'balance' in row and row['balance'] is not None:
                                    final_total += float(row['balance'])
                        
                        # Check conservation (allow small floating point tolerance)
                        if abs(final_total - initial_total) > 0.01:
                            validation_error = f"RACE CONDITION: Invariant '{inv_name}' FAILED! Final balance={final_total:.2f}, expected={initial_total:.2f} (money {'created' if final_total > initial_total else 'destroyed'}!)"
                            break
                    
                    # Check final state assertions (e.g., "read.GameServer.players.score == 60")
                    elif 'read.' in check_str and 'always' not in check_str:
                        # Simple final state check
                        passed = self._check_final_state_invariant(check_str, final_state)
                        if not passed:
                            validation_error = f"Invariant '{inv_name}' FAILED: {check_str}"
                            break
                    # LTL style: always(A -> eventually(B))
                    elif 'always' in check_str and 'eventually' in check_str:
                        import re
                        match = re.match(r"always\((.*)\s*->\s*eventually\((.*)\)\)", check_str)
                        if match:
                            cond_a = match.group(1).strip()
                            cond_b = match.group(2).strip()
                            passed = self._check_ltl_invariant(events, cond_a, cond_b, final_state)
                            if not passed:
                                validation_error = f"Invariant '{inv_name}' FAILED: {check_str}"
                                break
            
            # M6: Validate final state for critical errors (fuzzing-induced bugs)
            # This catches bugs that don't crash but violate business rules
            # Only run if YAML invariants didn't already find an error
            if not validation_error:
                try:
                    # Find all tables with balance columns (generic approach)
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = [row[0] for row in cursor.fetchall()]
                    
                    for table in tables:
                        # Skip internal SQLite tables and event_log
                        if table.startswith('sqlite_') or table == 'event_log' or table == 'simulation_metadata':
                            continue
                        
                        # Check if table has a balance column
                        try:
                            cursor.execute(f"SELECT balance FROM {table} LIMIT 0")
                            has_balance = True
                        except sqlite3.Error:
                            has_balance = False
                        
                        # Check if table has a score column (for game race condition detection)
                        try:
                            cursor.execute(f"SELECT score FROM {table} LIMIT 0")
                            has_score = True
                        except sqlite3.Error:
                            has_score = False
                        
                        # Auto-detect score-based race conditions when no YAML invariants defined
                        # This is a fallback for when users don't define explicit invariants
                        if has_score and not validation_error and not invariants:
                            # Count PlayerAction events to determine expected score
                            cursor.execute("SELECT COUNT(*) FROM event_log WHERE trigger_message = 'PlayerAction'")
                            action_count = cursor.fetchone()[0]
                            if action_count > 0:
                                expected_score = action_count * 10  # Each action adds 10
                                
                                # Get actual scores
                                cursor.execute(f"SELECT * FROM {table}")
                                col_names = [description[0] for description in cursor.description]
                                score_idx = col_names.index('score') if 'score' in col_names else -1
                                
                                if score_idx >= 0:
                                    cursor.execute(f"SELECT * FROM {table}")
                                    for row in cursor.fetchall():
                                        actual_score = row[score_idx]
                                        if actual_score is not None:
                                            try:
                                                actual_score = int(actual_score)
                                            except (ValueError, TypeError):
                                                continue
                                            if actual_score != expected_score:
                                                validation_error = f"RACE CONDITION: Lost Update in {table}! score={actual_score}, expected={expected_score} ({action_count} actions)"
                                                break
                        
                        if validation_error:
                            break
                        
                        if not has_balance:
                            continue
                        
                        # Check 1: Negative balances (overdraft bug)
                        if not validation_error:
                            cursor.execute(f"SELECT COUNT(*), MIN(balance) FROM {table} WHERE balance < 0")
                            row = cursor.fetchone()
                            if row[0] > 0:
                                validation_error = f"Negative balance in {table}: {row[0]} row(s), min={row[1]:.2f}"
                                break
                        
                        # Check 2: NULL balances (arithmetic error bug)
                        if not validation_error:
                            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE balance IS NULL")
                            null_count = cursor.fetchone()[0]
                            if null_count > 0:
                                validation_error = f"NULL balance in {table}: {null_count} row(s)"
                                break
                        
                        # Check 3: Infinity values (overflow bug)
                        if not validation_error:
                            import math
                            cursor.execute(f"SELECT * FROM {table}")
                            # Get column names
                            col_names = [description[0] for description in cursor.description]
                            if 'balance' in col_names:
                                balance_idx = col_names.index('balance')
                                # Check for infinity in balance column
                                cursor.execute(f"SELECT * FROM {table}")
                                for row in cursor.fetchall():
                                    balance = row[balance_idx]
                                    if balance is not None:
                                        try:
                                            balance_num = float(balance)
                                            if math.isinf(balance_num):
                                                validation_error = f"Infinite balance in {table}: {balance}"
                                                break
                                        except (ValueError, TypeError):
                                            pass
                        
                        if validation_error:
                            break
                    
                    # After checking all tables, do balance conservation check
                    # Auto-detect balance conservation when no YAML invariants and has balance tables
                    if not validation_error and not invariants:
                        # Get initial total from scenario
                        scenario = next((s for s in fuzzed_clotho_data.get('run', {}).get('scenarios', []) 
                                        if s.get('name') == self.scenario_name), None)
                        initial_total = 0.0
                        if scenario and 'initial_state' in scenario:
                            for init in scenario['initial_state']:
                                storage = init.get('storage', init.get('state', {}))
                                for table_name, rows in storage.items():
                                    for row in rows:
                                        if 'balance' in row:
                                            initial_total += float(row['balance'])
                        
                        if initial_total > 0:
                            # Calculate final total from all tables with balance columns
                            final_total = 0.0
                            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'event_log%' AND name NOT LIKE 'simulation_metadata%' AND name NOT LIKE 'sqlite_%'")
                            all_tables = [row[0] for row in cursor.fetchall()]
                            for tbl in all_tables:
                                try:
                                    cursor.execute(f"SELECT balance FROM {tbl}")
                                    for row in cursor.fetchall():
                                        if row[0] is not None:
                                            try:
                                                final_total += float(row[0])
                                            except (ValueError, TypeError):
                                                pass
                                except sqlite3.Error:
                                    pass
                            
                            # Check conservation (allow small floating point tolerance)
                            if abs(final_total - initial_total) > 0.01:
                                validation_error = f"RACE CONDITION: Balance not conserved! Final={final_total:.2f}, expected={initial_total:.2f} (money {'created' if final_total > initial_total else 'destroyed'}!)"
                            
                except Exception as e:
                    # If validation queries fail, that's okay - might be different schema
                    # or might be testing a completely different domain
                    pass
            
            conn.close()
            
            # If validation found issues, mark as failed
            if validation_error:
                return SimulationResult(
                    seed=seed,
                    success=False,
                    event_count=event_count,
                    execution_time_ms=execution_time,
                    db_path=sim.db_path,
                    state_fingerprints=state_fingerprints,
                    error_message=validation_error
                )
            
            return SimulationResult(
                seed=seed,
                success=True,
                event_count=event_count,
                execution_time_ms=execution_time,
                db_path=sim.db_path,
                state_fingerprints=state_fingerprints
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            return SimulationResult(
                seed=seed,
                success=False,
                event_count=0,
                execution_time_ms=execution_time,
                db_path="",
                error_message=str(e)
            )
    
    def _check_final_state_invariant(self, check_str: str, final_state: Dict) -> bool:
        """
        Check a final state invariant.
        
        Supports expressions like:
        - "read.TableName.column == value"
        - "read.TableName.column > 0"
        - "read.TableName.column < 100"
        """
        import re
        
        # Parse: read.Table.column op value
        match = re.match(r"read\.(\w+)\.(\w+)\s*(==|!=|>|<|>=|<=)\s*(.+)", check_str.strip())
        if not match:
            return True  # Can't parse, assume pass
        
        table_name, column, op, expected_str = match.groups()
        expected_str = expected_str.strip()
        
        # Get table data
        table_data = final_state.get(table_name, [])
        if not table_data:
            return True  # No data, can't fail
        
        # Parse expected value
        try:
            if expected_str.lower() in ('true', 'false'):
                expected = expected_str.lower() == 'true'
            elif '.' in expected_str:
                expected = float(expected_str)
            else:
                expected = int(expected_str)
        except:
            expected = expected_str  # Keep as string
        
        # Check all rows
        for row in table_data:
            if column not in row:
                continue
            actual = row[column]
            
            if op == '==' and actual != expected:
                return False
            elif op == '!=' and actual == expected:
                return False
            elif op == '>' and not (actual > expected):
                return False
            elif op == '<' and not (actual < expected):
                return False
            elif op == '>=' and not (actual >= expected):
                return False
            elif op == '<=' and not (actual <= expected):
                return False
        
        return True
    
    def _check_ltl_invariant(self, events: List[Dict], cond_a: str, cond_b: str, final_state: Dict) -> bool:
        """
        Check LTL-style invariant: always(A -> eventually(B))
        
        For every occurrence of event A, there must eventually be a matching event B.
        
        Supports conditions like:
        - "msg.MessageType" - matches event by trigger_message
        - "read.Table.column == value" - checks final state
        """
        import re
        
        # Find all indices where condition A is triggered
        a_indices = []
        for i, event in enumerate(events):
            if self._event_matches_condition(event, cond_a):
                a_indices.append(i)
        
        # For each A occurrence, check if B eventually happens
        for a_idx in a_indices:
            found_b = False
            
            # Check if B is a final state condition
            if cond_b.startswith('read.'):
                # Check final state
                found_b = self._check_final_state_invariant(cond_b, final_state)
            else:
                # Check events after A for condition B
                for j in range(a_idx + 1, len(events)):
                    if self._event_matches_condition(events[j], cond_b):
                        found_b = True
                        break
            
            if not found_b:
                return False
        
        return True
    
    def _event_matches_condition(self, event: Dict, condition: str) -> bool:
        """
        Check if an event matches a condition.
        
        Supports:
        - "msg.MessageType" - checks trigger_message
        - "msg.ComponentName.MessageType" - checks component + trigger
        """
        import re
        
        # msg.MessageType or msg.Component.MessageType
        if condition.startswith('msg.'):
            parts = condition[4:].split('.')
            if len(parts) == 1:
                # Just message type
                return event.get('trigger_message') == parts[0]
            elif len(parts) == 2:
                # Component.MessageType
                return (event.get('component') == parts[0] and 
                        event.get('trigger_message') == parts[1])
        
        return False
    
    def _apply_fuzzing(self, seed: int) -> Dict:
        """
        M6: Apply fuzzing to Clotho data
        
        CRITICAL FIX: Creates thread-local Fuzzer instances to avoid shared state
        
        Args:
            seed: Simulation seed (used for fuzzing randomness)
            
        Returns:
            Fuzzed Clotho data
        """
        import copy
        fuzzed_data = copy.deepcopy(self.clotho_data)
        
        # CRITICAL FIX: Create thread-local Fuzzer instances with derived seeds
        # This ensures each thread has independent random state
        input_fuzzer = InputFuzzer(FuzzingConfig(
            fuzz_inputs=self.fuzzing_config.fuzz_inputs,
            boundary_value_prob=self.fuzzing_config.boundary_value_prob,
            type_confusion_prob=self.fuzzing_config.type_confusion_prob,
            null_prob=self.fuzzing_config.null_prob,
            extreme_value_prob=self.fuzzing_config.extreme_value_prob,
            seed=seed + 1  # Derived from simulation seed
        )) if self.fuzzing_config.fuzz_inputs else None
        
        state_fuzzer = StateFuzzer(FuzzingConfig(
            fuzz_states=self.fuzzing_config.fuzz_states,
            boundary_value_prob=self.fuzzing_config.boundary_value_prob,
            type_confusion_prob=self.fuzzing_config.type_confusion_prob,
            null_prob=self.fuzzing_config.null_prob,
            extreme_value_prob=self.fuzzing_config.extreme_value_prob,
            seed=seed + 2  # Derived from simulation seed
        )) if self.fuzzing_config.fuzz_states else None
        
        # Find the scenario
        scenarios = fuzzed_data.get('scenarios', [])
        scenario = next((s for s in scenarios if s.get('name') == self.scenario_name), None)
        
        if not scenario:
            return fuzzed_data
        
        # Apply state fuzzing
        if state_fuzzer and 'initial_state' in scenario:
            scenario['initial_state'] = state_fuzzer.fuzz_initial_state(scenario['initial_state'])
        
        # Apply input fuzzing to payloads
        if input_fuzzer and 'steps' in scenario:
            for step in scenario['steps']:
                if 'payload' in step:
                    step['payload'] = input_fuzzer.fuzz_payload(step['payload'])
        
        return fuzzed_data
    
    def run_batch(
        self,
        num_simulations: int,
        seed_start: Optional[int] = None,
        progress_callback: Optional[Callable[[int, int, SimulationResult], None]] = None,
        cleanup_dbs: bool = True
    ) -> tuple[List[SimulationResult], ChaosMatrixStats]:
        """
        Run batch of simulations with different seeds
        
        Args:
            num_simulations: Number of simulations to run
            seed_start: Starting seed (default: random)
            progress_callback: Called after each simulation: (completed, total, result)
            cleanup_dbs: Delete DB files after success (keep failures for debugging)
            
        Returns:
            (results, stats) tuple
        """
        # Generate seeds
        if seed_start is None:
            seed_start = random.randint(0, 2**31)
        
        seeds = [seed_start + i for i in range(num_simulations)]
        
        results: List[SimulationResult] = []
        start_time = time.time()
        
        # Run simulations in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_seed = {
                executor.submit(self._run_single_simulation, seed): seed 
                for seed in seeds
            }
            
            # Collect results as they complete
            for completed, future in enumerate(concurrent.futures.as_completed(future_to_seed), 1):
                result = future.result()
                results.append(result)
                
                # CRITICAL FIX: Robust DB cleanup with retry for Windows file locks
                if cleanup_dbs and result.success and os.path.exists(result.db_path):
                    import time as time_module
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            os.remove(result.db_path)
                            break  # Success
                        except PermissionError:
                            # Windows file lock - wait and retry
                            if attempt < max_retries - 1:
                                time_module.sleep(0.1)
                            # On final attempt, just skip
                        except Exception:
                            # Other errors - skip silently
                            break
                
                # Progress callback
                if progress_callback:
                    progress_callback(completed, num_simulations, result)
        
        total_time = (time.time() - start_time) * 1000
        
        # Calculate statistics
        stats = self._calculate_stats(results, total_time)
        
        return results, stats
    
    def _calculate_stats(self, results: List[SimulationResult], total_time_ms: float) -> ChaosMatrixStats:
        """Calculate aggregated statistics from results (M5: includes reliability scoring)"""
        completed = len([r for r in results if r.success])
        failed = len([r for r in results if not r.success])
        success_rate = (completed / len(results)) * 100 if results else 0
        
        execution_times = [r.execution_time_ms for r in results if r.success]
        avg_time = sum(execution_times) / len(execution_times) if execution_times else 0
        min_time = min(execution_times) if execution_times else 0
        max_time = max(execution_times) if execution_times else 0
        
        failing_seeds = [r.seed for r in results if not r.success]
        
        # Group failures by error message to find unique patterns
        error_patterns = set()
        for r in results:
            if not r.success and r.error_message:
                # Normalize error message (remove specific values)
                error_patterns.add(r.error_message.split('\n')[0])  # First line only
        
        failure_messages = list(error_patterns)
        
        # M5: Compute reliability score if coverage tracking enabled
        reliability_score = None
        unique_states = None
        total_state_observations = None
        state_coverage_rate = None
        
        if self.track_coverage and self.coverage_tracker:
            # Collect all fingerprints from successful runs
            for result in results:
                if result.success and result.state_fingerprints:
                    for fp in result.state_fingerprints:
                        self.coverage_tracker.add_state(fp)
            
            # Get coverage statistics
            coverage = self.coverage_tracker.get_coverage_stats()
            
            # Compute reliability score
            scorer = ReliabilityScorer()
            score = scorer.compute_score(coverage, len(results))
            
            reliability_score = score.score
            unique_states = score.unique_states
            total_state_observations = score.total_observations
            state_coverage_rate = coverage.coverage_rate
        
        return ChaosMatrixStats(
            total_runs=len(results),
            completed=completed,
            failed=failed,
            success_rate=success_rate,
            total_execution_time_ms=total_time_ms,
            avg_execution_time_ms=avg_time,
            min_execution_time_ms=min_time,
            max_execution_time_ms=max_time,
            failing_seeds=failing_seeds,
            unique_failure_patterns=len(error_patterns),
            failure_messages=failure_messages,
            reliability_score=reliability_score,
            unique_states=unique_states,
            total_state_observations=total_state_observations,
            state_coverage_rate=state_coverage_rate
        )
    
    def find_divergent_states(
        self,
        num_simulations: int = 100,
        state_extractor: Optional[Callable[[str], Dict]] = None
    ) -> Dict[str, List[int]]:
        """
        M4 Advanced: Find simulations that result in different final states
        
        This helps identify non-deterministic bugs where different event
        interleavings lead to different outcomes.
        
        Args:
            num_simulations: Number of simulations to run
            state_extractor: Function to extract comparable state from DB path
            
        Returns:
            Dictionary mapping state hashes to lists of seeds that produced them
        """
        if state_extractor is None:
            state_extractor = self._default_state_extractor
        
        results, _ = self.run_batch(num_simulations, cleanup_dbs=False)
        
        # Group by final state
        state_to_seeds: Dict[str, List[int]] = {}
        
        for result in results:
            if result.success:
                try:
                    final_state = state_extractor(result.db_path)
                    state_hash = str(sorted(final_state.items()))  # Simple hash
                    
                    if state_hash not in state_to_seeds:
                        state_to_seeds[state_hash] = []
                    state_to_seeds[state_hash].append(result.seed)
                except:
                    pass  # Skip if state extraction fails
        
        return state_to_seeds
    
    def _default_state_extractor(self, db_path: str) -> Dict:
        """Extract all table data as final state"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all user tables (not event_log or simulation_metadata)
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            AND name NOT IN ('event_log', 'simulation_metadata')
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        state = {}
        for table in tables:
            cursor.execute(f"SELECT * FROM {table}")
            state[table] = cursor.fetchall()
        
        conn.close()
        return state


def print_chaos_matrix_report(stats: ChaosMatrixStats, verbose: bool = False):
    """Print formatted Chaos Matrix statistics"""
    print("\n" + "=" * 80)
    print("CHAOS MATRIX RESULTS")
    print("=" * 80)
    print(f"\nTotal Simulations: {stats.total_runs}")
    print(f"  ✅ Successful: {stats.completed} ({stats.success_rate:.1f}%)")
    print(f"  ❌ Failed: {stats.failed}")
    
    print(f"\nExecution Time:")
    print(f"  Total: {stats.total_execution_time_ms:.2f}ms ({stats.total_execution_time_ms/1000:.2f}s)")
    print(f"  Average per simulation: {stats.avg_execution_time_ms:.2f}ms")
    print(f"  Range: {stats.min_execution_time_ms:.2f}ms - {stats.max_execution_time_ms:.2f}ms")
    
    if stats.failed > 0:
        print(f"\n⚠️  FAILURES DETECTED:")
        print(f"  Unique failure patterns: {stats.unique_failure_patterns}")
        print(f"  Failing seeds (for replay):")
        for seed in stats.failing_seeds[:10]:  # Show first 10
            print(f"    - {seed}")
        if len(stats.failing_seeds) > 10:
            print(f"    ... and {len(stats.failing_seeds) - 10} more")
    
    print("\n" + "=" * 80)
