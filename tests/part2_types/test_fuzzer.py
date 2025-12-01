"""
M6: Statistical Fuzzing Tests
Tests input/state/scenario fuzzing strategies
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
import copy
from core.chaos.fuzzer import (
    FuzzingConfig,
    InputFuzzer,
    StateFuzzer,
    ScenarioFuzzer
)


class TestInputFuzzer:
    """Test input fuzzing strategies"""

    def test_fuzz_integer_boundary_values(self):
        """Test that integer fuzzing can produce boundary values"""
        config = FuzzingConfig(
            fuzz_inputs=True,
            boundary_value_prob=1.0,  # Always use boundary values
            type_confusion_prob=0.0,
            null_prob=0.0,
            seed=42
        )
        fuzzer = InputFuzzer(config)
        
        # Fuzz many times to ensure we hit various boundary values
        results = set()
        for i in range(100):
            config_copy = FuzzingConfig(
                fuzz_inputs=True,
                boundary_value_prob=1.0,
                type_confusion_prob=0.0,
                null_prob=0.0,
                seed=i
            )
            fuzzer_temp = InputFuzzer(config_copy)
            fuzzed = fuzzer_temp.fuzz_payload({"amount": 100})
            results.add(fuzzed["amount"])
        
        # Should include some boundary values
        boundary_values = {0, -1, 2147483647}
        assert len(boundary_values.intersection(results)) > 0, \
            f"Expected boundary values in {results}"

    def test_fuzz_integer_type_confusion(self):
        """Test that integer fuzzing can produce type confusion (int -> string)"""
        config = FuzzingConfig(
            fuzz_inputs=True,
            boundary_value_prob=0.0,
            type_confusion_prob=1.0,  # Always use type confusion
            null_prob=0.0
        )
        
        # Fuzz many times to find type confusion
        for i in range(50):
            config_copy = FuzzingConfig(
                fuzz_inputs=True,
                boundary_value_prob=0.0,
                type_confusion_prob=1.0,
                null_prob=0.0,
                seed=i
            )
            fuzzer = InputFuzzer(config_copy)
            fuzzed = fuzzer.fuzz_payload({"user_id": 42})
            if isinstance(fuzzed["user_id"], str):
                # Found type confusion!
                return
        
        pytest.fail("Did not find type confusion after 50 attempts")

    def test_fuzz_string_empty(self):
        """Test that string fuzzing can produce empty strings"""
        config = FuzzingConfig(
            fuzz_inputs=True,
            boundary_value_prob=1.0,  # Boundary for strings = empty
            type_confusion_prob=0.0,
            null_prob=0.0
        )
        
        # Fuzz many times
        results = []
        for i in range(50):
            config_copy = FuzzingConfig(
                fuzz_inputs=True,
                boundary_value_prob=1.0,
                type_confusion_prob=0.0,
                null_prob=0.0,
                seed=i
            )
            fuzzer = InputFuzzer(config_copy)
            fuzzed = fuzzer.fuzz_payload({"name": "Alice"})
            results.append(fuzzed["name"])
        
        # Should produce at least one empty string
        assert "" in results, f"Expected empty string in {results}"

    def test_fuzz_boolean(self):
        """Test that boolean fuzzing flips values"""
        config = FuzzingConfig(
            fuzz_inputs=True,
            boundary_value_prob=0.5,  # 50% chance to flip
            type_confusion_prob=0.0,
            null_prob=0.0
        )
        
        # Fuzz many times
        original_value = True
        results = []
        for i in range(50):
            config_copy = FuzzingConfig(
                fuzz_inputs=True,
                boundary_value_prob=0.5,
                type_confusion_prob=0.0,
                null_prob=0.0,
                seed=i
            )
            fuzzer = InputFuzzer(config_copy)
            fuzzed = fuzzer.fuzz_payload({"active": original_value})
            results.append(fuzzed["active"])
        
        # Should produce both True and False
        assert True in results and False in results, \
            f"Expected both True and False in {results}"

    def test_fuzz_null_injection(self):
        """Test that null injection can replace values with None"""
        config = FuzzingConfig(
            fuzz_inputs=True,
            boundary_value_prob=0.0,
            type_confusion_prob=0.0,
            null_prob=1.0,  # Always inject null
            seed=42
        )
        
        # Fuzz and check for null
        for i in range(30):
            config_copy = FuzzingConfig(
                fuzz_inputs=True,
                boundary_value_prob=0.0,
                type_confusion_prob=0.0,
                null_prob=1.0,
                seed=i
            )
            fuzzer = InputFuzzer(config_copy)
            fuzzed = fuzzer.fuzz_payload({"email": "test@example.com"})
            if fuzzed["email"] is None:
                return
        
        pytest.fail("Did not find null injection after 30 attempts")

    def test_fuzz_nested_structures(self):
        """Test that fuzzing works on nested dictionaries"""
        config = FuzzingConfig(
            fuzz_inputs=True,
            boundary_value_prob=0.3,
            type_confusion_prob=0.2,
            null_prob=0.1,
            seed=42
        )
        fuzzer = InputFuzzer(config)
        
        nested = {
            "user": {
                "id": 123,
                "profile": {
                    "age": 25,
                    "name": "Bob"
                }
            },
            "items": [1, 2, 3]
        }
        
        fuzzed = fuzzer.fuzz_payload(nested)
        
        # Ensure structure is preserved
        assert "user" in fuzzed
        assert "profile" in fuzzed["user"]
        assert "items" in fuzzed


class TestStateFuzzer:
    """Test state fuzzing strategies"""

    def test_fuzz_initial_state_empty_tables(self):
        """Test that state fuzzing can produce empty tables"""
        # Try many times to ensure we hit empty table probability
        for i in range(20):
            config = FuzzingConfig(
                fuzz_states=True,
                seed=i
            )
            fuzzer = StateFuzzer(config)
            
            initial_state = [{
                "component": "DB",
                "state": {
                    "users": [
                        {"id": 1, "balance": 100},
                        {"id": 2, "balance": 200}
                    ],
                    "orders": [
                        {"order_id": 1, "total": 50}
                    ]
                }
            }]
            
            fuzzed = fuzzer.fuzz_initial_state(initial_state)
            state_data = fuzzed[0]["state"]
            
            # Should eventually produce empty tables
            if len(state_data["users"]) == 0 or len(state_data["orders"]) == 0:
                return
        
        pytest.fail("Did not produce empty tables after 20 attempts")

    def test_fuzz_initial_state_large_datasets(self):
        """Test that state fuzzing can produce large datasets"""
        # Try many times to hit large dataset probability
        for i in range(20):
            config = FuzzingConfig(
                fuzz_states=True,
                seed=i
            )
            fuzzer = StateFuzzer(config)
            
            initial_state = [{
                "component": "DB",
                "state": {
                    "products": [
                        {"id": 1, "stock": 10},
                        {"id": 2, "stock": 20}
                    ]
                }
            }]
            
            fuzzed = fuzzer.fuzz_initial_state(initial_state)
            state_data = fuzzed[0]["state"]
            
            # Should eventually produce significantly more records
            if len(state_data["products"]) > len(initial_state[0]["state"]["products"]):
                return
        
        pytest.fail("Did not produce large dataset after 20 attempts")

    def test_fuzz_numeric_fields(self):
        """Test that state fuzzing randomizes numeric fields"""
        initial_state = [{
            "component": "DB",
            "state": {
                "accounts": [
                    {"account_id": 1, "balance": 1000.0, "credit_limit": 500}
                ]
            }
        }]
        
        # Fuzz multiple times to get different values
        balances = set()
        for i in range(50):
            config = FuzzingConfig(fuzz_states=True, seed=i)
            fuzzer = StateFuzzer(config)
            fuzzed = fuzzer.fuzz_initial_state(initial_state)
            state_data = fuzzed[0]["state"]
            if state_data["accounts"]:  # Not empty
                balances.add(state_data["accounts"][0]["balance"])
        
        # Should produce diverse balance values
        assert len(balances) > 5, f"Expected diverse balances, got {len(balances)} unique values"

    def test_fuzz_preserves_table_names(self):
        """Test that state fuzzing preserves table structure"""
        config = FuzzingConfig(fuzz_states=True, seed=77)
        fuzzer = StateFuzzer(config)
        
        initial_state = [{
            "component": "DB",
            "state": {
                "users": [{"id": 1}],
                "orders": [{"order_id": 1}],
                "inventory": [{"product_id": 1}]
            }
        }]
        
        fuzzed = fuzzer.fuzz_initial_state(initial_state)
        state_data = fuzzed[0]["state"]
        
        # All table names should be preserved
        assert set(state_data.keys()) == set(initial_state[0]["state"].keys())


class TestScenarioFuzzer:
    """Test scenario chaining strategies"""

    def test_chain_sequential(self):
        """Test sequential scenario chaining"""
        config = FuzzingConfig(fuzz_scenarios=True, seed=1)
        fuzzer = ScenarioFuzzer(config)
        
        Clotho1 = {
            "name": "Create User",
            "steps": [
                {"send": "CreateUser", "to": "DB", "payload": {"user_id": 1}}
            ]
        }
        Clotho2 = {
            "name": "Create Order",
            "steps": [
                {"send": "CreateOrder", "to": "DB", "payload": {"order_id": 1}}
            ]
        }
        
        chained = fuzzer.chain_scenarios([Clotho1, Clotho2], mode="sequential")
        
        # Should have all events in order
        assert len(chained["steps"]) == 2
        assert chained["steps"][0]["send"] == "CreateUser"
        assert chained["steps"][1]["send"] == "CreateOrder"

    def test_chain_parallel(self):
        """Test parallel scenario chaining (shuffle)"""
        config = FuzzingConfig(fuzz_scenarios=True, seed=5)
        fuzzer = ScenarioFuzzer(config)
        
        Clotho1 = {
            "name": "A",
            "steps": [
                {"send": "A", "payload": {}},
                {"send": "B", "payload": {}}
            ]
        }
        Clotho2 = {
            "name": "B",
            "steps": [
                {"send": "C", "payload": {}},
                {"send": "D", "payload": {}}
            ]
        }
        
        chained = fuzzer.chain_scenarios([Clotho1, Clotho2], mode="parallel")
        
        # Should have all events but potentially shuffled
        assert len(chained["steps"]) == 4
        event_types = [m["send"] for m in chained["steps"]]
        assert set(event_types) == {"A", "B", "C", "D"}
        
        # Order may be different from sequential
        # (not guaranteed, but statistically likely with enough scenarios)

    def test_chain_interleaved(self):
        """Test interleaved scenario chaining (round-robin)"""
        config = FuzzingConfig(fuzz_scenarios=True, seed=10)
        fuzzer = ScenarioFuzzer(config)
        
        Clotho1 = {
            "name": "A",
            "steps": [
                {"send": "A1", "payload": {}},
                {"send": "A2", "payload": {}}
            ]
        }
        Clotho2 = {
            "name": "B",
            "steps": [
                {"send": "B1", "payload": {}},
                {"send": "B2", "payload": {}}
            ]
        }
        
        chained = fuzzer.chain_scenarios([Clotho1, Clotho2], mode="interleaved")
        
        # Should interleave: A1, B1, A2, B2
        assert len(chained["steps"]) == 4
        types = [m["send"] for m in chained["steps"]]
        
        # Check interleaving pattern
        assert types[0] in ["A1", "B1"]  # First from one scenario
        assert types[1] in ["A1", "B1"]  # First from other scenario
        assert types[0] != types[1]      # Not the same

    def test_chain_preserves_initial_state(self):
        """Test that chaining preserves initial_state from first scenario"""
        config = FuzzingConfig(fuzz_scenarios=True, seed=20)
        fuzzer = ScenarioFuzzer(config)
        
        Clotho1 = {
            "name": "A",
            "initial_state": [{
                "component": "ServiceA",
                "state": {"table1": [{"id": 1}]}
            }],
            "steps": [{"send": "Test1", "payload": {}}]
        }
        Clotho2 = {
            "name": "B",
            "initial_state": [{
                "component": "ServiceB",
                "state": {"table2": [{"id": 2}]}
            }],
            "steps": [{"send": "Test2", "payload": {}}]
        }
        
        chained = fuzzer.chain_scenarios([Clotho1, Clotho2], mode="sequential")
        
        # Should use initial_state from first scenario
        assert chained["initial_state"] == Clotho1["initial_state"]

    def test_chain_single_scenario(self):
        """Test chaining with single scenario returns it unchanged"""
        config = FuzzingConfig(fuzz_scenarios=True, seed=30)
        fuzzer = ScenarioFuzzer(config)
        
        Clotho = {
            "name": "Single",
            "steps": [{"send": "Single", "payload": {}}]
        }
        
        chained = fuzzer.chain_scenarios([Clotho], mode="sequential")
        
        # Should be identical
        assert chained == Clotho


class TestFuzzingIntegration:
    """Integration tests for combined fuzzing strategies"""

    def test_full_fuzzing_workflow(self):
        """Test complete fuzzing workflow: state + input"""
        config = FuzzingConfig(
            fuzz_inputs=True,
            fuzz_states=True,
            fuzz_scenarios=False,
            boundary_value_prob=0.3,
            type_confusion_prob=0.2,
            null_prob=0.1,
            seed=100
        )
        
        input_fuzzer = InputFuzzer(config)
        state_fuzzer = StateFuzzer(config)
        
        # Simulate a complete fuzzing pass
        initial_state = [{
            "component": "DB",
            "state": {
                "users": [{"id": 1, "balance": 100}]
            }
        }]
        message = {
            "type": "Transfer",
            "payload": {"from": 1, "to": 2, "amount": 50}
        }
        
        fuzzed_state = state_fuzzer.fuzz_initial_state(initial_state)
        fuzzed_message = input_fuzzer.fuzz_payload(message["payload"])
        
        # Both should potentially be modified
        assert fuzzed_state is not initial_state
        assert fuzzed_message is not message["payload"]

    def test_fuzzing_determinism(self):
        """Test that same seed produces same fuzzed output"""
        # For determinism test, we need to reset random state each time
        message = {"user_id": 123, "amount": 500}
        
        # Fuzz twice with same seed
        config1 = FuzzingConfig(
            fuzz_inputs=True,
            boundary_value_prob=0.5,
            type_confusion_prob=0.3,
            null_prob=0.2,
            seed=42
        )
        fuzzer1 = InputFuzzer(config1)
        result1 = fuzzer1.fuzz_payload(copy.deepcopy(message))
        
        config2 = FuzzingConfig(
            fuzz_inputs=True,
            boundary_value_prob=0.5,
            type_confusion_prob=0.3,
            null_prob=0.2,
            seed=42
        )
        fuzzer2 = InputFuzzer(config2)
        result2 = fuzzer2.fuzz_payload(copy.deepcopy(message))
        
        # Should be identical
        assert result1 == result2, f"Expected deterministic output: {result1} vs {result2}"

    def test_fuzzing_non_determinism(self):
        """Test that different seeds produce different outputs"""
        message = {"user_id": 123, "amount": 500}
        
        # Fuzz with different seeds
        results = set()
        for i in range(20):
            config = FuzzingConfig(
                fuzz_inputs=True,
                boundary_value_prob=0.5,
                type_confusion_prob=0.3,
                null_prob=0.2,
                seed=i
            )
            fuzzer = InputFuzzer(config)
            fuzzed = fuzzer.fuzz_payload(message)
            # Convert to string for hashing
            results.add(str(sorted(fuzzed.items())))
        
        # Should produce diverse outputs
        assert len(results) > 5, f"Expected diverse outputs, got {len(results)} unique"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
