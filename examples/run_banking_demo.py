#!/usr/bin/env python3
"""
Clotho Engine Demo: Banking Vulnerability Detection

This script demonstrates how Clotho's ChaosMatrix + Statistical Fuzzing
can automatically discover vulnerabilities in a banking system that
appears bug-free under normal testing.

Vulnerabilities demonstrated:
1. Negative amount exploit (money creation)
2. Missing balance validation (overdraft)
3. Frozen account bypass

Run:
    python examples/run_banking_demo.py
"""

import os
import sys
from pathlib import Path

# Add project root to path (in case running directly)
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import yaml
from core.chaos.chaos_matrix import ChaosMatrix, print_chaos_matrix_report
from core.chaos.fuzzer import FuzzingConfig


def load_scenario(yaml_path: str) -> dict:
    """Load Clotho YAML scenario file."""
    with open(yaml_path, 'r') as f:
        return yaml.safe_load(f)


def run_baseline_test(clotho_data: dict, scenario_name: str) -> None:
    """Run baseline test without fuzzing - appears to work perfectly."""
    print("\n" + "=" * 80)
    print("PHASE 1: BASELINE TEST (No Fuzzing)")
    print("=" * 80)
    print("Running 10 simulations with normal inputs...")
    print("This simulates traditional unit testing.\n")
    
    # No fuzzing - just run with different seeds
    matrix = ChaosMatrix(
        clotho_data=clotho_data,
        scenario_name=scenario_name,
        max_workers=4,
        track_coverage=True,
        fuzzing_config=FuzzingConfig(
            fuzz_inputs=False,
            fuzz_states=False,
            fuzz_scenarios=False
        )
    )
    
    results, stats = matrix.run_batch(
        num_simulations=10,
        seed_start=1000,
        cleanup_dbs=True
    )
    
    print_chaos_matrix_report(stats)
    
    if stats.success_rate == 100.0:
        print("\n✅ BASELINE: All tests pass! System appears bug-free.")
        print("   But wait... let's try fuzzing the inputs...")
    else:
        print(f"\n⚠️ BASELINE: {stats.failed} failures detected even without fuzzing!")


def run_fuzzing_test(clotho_data: dict, scenario_name: str) -> None:
    """Run with statistical fuzzing - reveals hidden vulnerabilities."""
    print("\n" + "=" * 80)
    print("PHASE 2: STATISTICAL FUZZING (Input + State)")
    print("=" * 80)
    print("Running 50 simulations with fuzzed inputs...")
    print("Fuzzing strategies: boundary values, type confusion, null injection\n")
    
    # Enable full fuzzing
    fuzzing_config = FuzzingConfig(
        fuzz_inputs=True,
        fuzz_states=True,
        fuzz_scenarios=False,
        boundary_value_prob=0.3,   # 30% chance of boundary values (-1, 0, MAX_INT)
        type_confusion_prob=0.1,   # 10% chance of type confusion (int -> str)
        null_prob=0.05,            # 5% chance of null injection
        extreme_value_prob=0.2     # 20% chance of extreme values
    )
    
    matrix = ChaosMatrix(
        clotho_data=clotho_data,
        scenario_name=scenario_name,
        max_workers=4,
        track_coverage=True,
        fuzzing_config=fuzzing_config
    )
    
    def progress_callback(completed: int, total: int, result):
        """Show progress during simulation."""
        status = "✓" if result.success else "✗"
        if not result.success and result.error_message:
            # Show first line of error
            error_preview = result.error_message.split('\n')[0][:60]
            print(f"  [{completed}/{total}] Seed {result.seed}: {status} - {error_preview}")
        elif completed % 10 == 0:
            print(f"  [{completed}/{total}] Running...")
    
    results, stats = matrix.run_batch(
        num_simulations=50,
        seed_start=2000,
        progress_callback=progress_callback,
        cleanup_dbs=True
    )
    
    print_chaos_matrix_report(stats)
    
    # Show detailed failure analysis
    if stats.failed > 0:
        print("\n" + "=" * 80)
        print("VULNERABILITY ANALYSIS")
        print("=" * 80)
        
        # Categorize failures by type
        failure_categories = {}
        for result in results:
            if not result.success and result.error_message:
                # Extract failure category
                msg = result.error_message
                if "Negative balance" in msg:
                    category = "💰 NEGATIVE BALANCE (Overdraft Bug)"
                elif "money created" in msg or "money destroyed" in msg:
                    category = "💸 MONEY CREATION/DESTRUCTION (Conservation Violation)"
                elif "NULL balance" in msg:
                    category = "❓ NULL ARITHMETIC ERROR"
                elif "RACE CONDITION" in msg:
                    category = "🔄 RACE CONDITION"
                elif "Infinite balance" in msg:
                    category = "♾️ INTEGER OVERFLOW"
                else:
                    category = "❌ OTHER ERROR"
                
                if category not in failure_categories:
                    failure_categories[category] = []
                failure_categories[category].append(result)
        
        print("\nVulnerabilities Found by Category:")
        print("-" * 40)
        for category, failures in sorted(failure_categories.items()):
            print(f"\n{category}")
            print(f"  Occurrences: {len(failures)}")
            print(f"  Example seed for replay: {failures[0].seed}")
            print(f"  Error: {failures[0].error_message.split(chr(10))[0][:70]}")
    
    # Reliability Score
    print("\n" + "=" * 80)
    print("RELIABILITY ASSESSMENT")
    print("=" * 80)
    
    if stats.reliability_score is not None:
        score = stats.reliability_score
        if score >= 0.95:
            grade = "A (Production Ready)"
        elif score >= 0.80:
            grade = "B (Needs Hardening)"
        elif score >= 0.60:
            grade = "C (Significant Issues)"
        elif score >= 0.40:
            grade = "D (Critical Bugs)"
        else:
            grade = "F (Unsafe)"
        
        print(f"\n  Reliability Score: {score:.1%}")
        print(f"  Grade: {grade}")
        print(f"  Unique States Explored: {stats.unique_states}")
        print(f"  State Coverage Rate: {stats.state_coverage_rate:.1%}" if stats.state_coverage_rate else "")
    
    print("\n" + "=" * 80)
    print("CONCLUSION")
    print("=" * 80)
    
    if stats.failed > 0:
        print("""
🐛 BUGS FOUND! The banking system has critical vulnerabilities:

   1. NO NEGATIVE AMOUNT VALIDATION
      → Sending amount=-100 CREATES money from nothing!
      
   2. NO BALANCE CHECK BEFORE WITHDRAWAL  
      → Can withdraw more than available balance!
      
   3. NO ACCOUNT STATUS CHECK
      → Can deposit/withdraw from frozen accounts!

📋 RECOMMENDED FIXES:
   - Add validation: amount > 0
   - Add check: balance >= withdrawal_amount  
   - Add check: account.status == 'active'
   
🔄 To replay a failing scenario:
   sim = Simulator(clotho_data, simulation_seed=<failing_seed>)
""")
    else:
        print("\n✅ No vulnerabilities found in this fuzzing run.")
        print("   Consider increasing simulation count or fuzzing probabilities.")


def main():
    """Main entry point for the demo."""
    print("""
╔═══════════════════════════════════════════════════════════════════════════════╗
║                                                                               ║
║   ██████╗██╗      ██████╗ ████████╗██╗  ██╗ ██████╗     ███████╗███╗   ██╗   ║
║  ██╔════╝██║     ██╔═══██╗╚══██╔══╝██║  ██║██╔═══██╗    ██╔════╝████╗  ██║   ║
║  ██║     ██║     ██║   ██║   ██║   ███████║██║   ██║    █████╗  ██╔██╗ ██║   ║
║  ██║     ██║     ██║   ██║   ██║   ██╔══██║██║   ██║    ██╔══╝  ██║╚██╗██║   ║
║  ╚██████╗███████╗╚██████╔╝   ██║   ██║  ██║╚██████╔╝    ███████╗██║ ╚████║   ║
║   ╚═════╝╚══════╝ ╚═════╝    ╚═╝   ╚═╝  ╚═╝ ╚═════╝     ╚══════╝╚═╝  ╚═══╝   ║
║                                                                               ║
║           HFT-Grade Deterministic Simulation & Fuzzing Engine                 ║
║                                                                               ║
╚═══════════════════════════════════════════════════════════════════════════════╝
""")
    
    # Locate scenario file
    script_dir = Path(__file__).parent
    scenario_path = script_dir / "test_banking_scenario.yaml"
    
    if not scenario_path.exists():
        print(f"❌ Error: Scenario file not found: {scenario_path}")
        print("   Please ensure test_banking_scenario.yaml is in the examples/ directory.")
        sys.exit(1)
    
    print(f"📂 Loading scenario: {scenario_path}")
    clotho_data = load_scenario(str(scenario_path))
    
    # Extract scenario name from the YAML
    scenarios = clotho_data.get('run', {}).get('scenarios', [])
    if not scenarios:
        print("❌ Error: No scenarios found in YAML file.")
        sys.exit(1)
    
    scenario_name = scenarios[0].get('name', 'default')
    print(f"🎯 Scenario: {scenario_name}")
    
    # Run baseline (no fuzzing)
    run_baseline_test(clotho_data, scenario_name)
    
    # Run with fuzzing
    run_fuzzing_test(clotho_data, scenario_name)
    
    print("\n✨ Demo complete! See README.md for more information.\n")


if __name__ == "__main__":
    main()
