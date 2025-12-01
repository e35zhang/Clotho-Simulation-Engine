"""
core/coverage_tracker.py

M5: State Coverage Tracker
Tracks unique states visited across simulations for reliability scoring
"""
import hashlib
import json
from typing import Set, Dict, Optional
from dataclasses import dataclass


@dataclass
class CoverageStats:
    """Statistics about state space coverage"""
    unique_states: int
    total_observations: int
    coverage_rate: float  # unique / total


class CoverageTracker:
    """
    Tracks state fingerprints across multiple simulation runs.
    
    State Fingerprint: SHA-256 hash of all component states at a point in time.
    Used to measure state space exploration in Chaos Matrix testing.
    """
    
    def __init__(self):
        self.state_fingerprints: Set[str] = set()
        self.observation_count: int = 0
    
    def add_state(self, state_fingerprint: str) -> bool:
        """
        Add a state fingerprint to the tracker.
        
        Args:
            state_fingerprint: SHA-256 hash of system state
            
        Returns:
            True if this is a NEW unique state, False if already seen
        """
        self.observation_count += 1
        is_new = state_fingerprint not in self.state_fingerprints
        
        if is_new:
            self.state_fingerprints.add(state_fingerprint)
        
        return is_new
    
    def get_coverage_stats(self) -> CoverageStats:
        """
        Get current coverage statistics.
        
        Returns:
            CoverageStats with unique states, observations, and rate
        """
        unique = len(self.state_fingerprints)
        total = self.observation_count
        rate = unique / total if total > 0 else 0.0
        
        return CoverageStats(
            unique_states=unique,
            total_observations=total,
            coverage_rate=rate
        )
    
    def estimate_total_states(self) -> Optional[int]:
        """
        Estimate total possible states using Good-Turing frequency estimation.
        
        This is a statistical method to estimate "how many states haven't we seen yet?"
        based on the rate of discovering new states.
        
        Returns:
            Estimated total state space size, or None if insufficient data
        """
        if self.observation_count < 100:
            # Need at least 100 observations for reasonable estimate
            return None
        
        unique = len(self.state_fingerprints)
        
        # Simple estimation: If we've seen U unique states in N observations,
        # and the discovery rate is slowing, estimate total as U / coverage_rate
        # where coverage_rate approaches 1.0 as we explore more
        
        # Use Heaps' Law approximation: V = K * N^β
        # where V = unique states, N = observations, K and β are constants
        # For distributed systems, β typically ranges from 0.4 to 0.6
        
        beta = 0.5  # Conservative middle estimate
        k = unique / (self.observation_count ** beta)
        
        # Estimate we need to observe ~10x current to reach 95% coverage
        estimated_total = int(k * ((self.observation_count * 10) ** beta))
        
        return max(estimated_total, unique)  # At least what we've seen
    
    def reset(self):
        """Clear all tracked states (for new test session)"""
        self.state_fingerprints.clear()
        self.observation_count = 0
    
    def merge(self, other: 'CoverageTracker'):
        """
        Merge another tracker's states into this one.
        Useful for combining results from parallel simulations.
        """
        self.state_fingerprints.update(other.state_fingerprints)
        self.observation_count += other.observation_count


def compute_state_fingerprint(component_states: Dict[str, Dict]) -> str:
    """
    Compute SHA-256 fingerprint of all component states.
    
    Args:
        component_states: Dict mapping table_name -> {rows as dicts}
        Example: {
            'account': [{'id': 'alice', 'balance': 1000}, ...],
            'transaction': [{'tx_id': 'tx1', 'status': 'completed'}, ...]
        }
    
    Returns:
        Hex string of SHA-256 hash (64 characters)
    """
    # Convert to canonical JSON (sorted keys, no whitespace)
    canonical_json = json.dumps(component_states, sort_keys=True, separators=(',', ':'))
    
    # Compute SHA-256 hash
    hash_obj = hashlib.sha256(canonical_json.encode('utf-8'))
    
    return hash_obj.hexdigest()
