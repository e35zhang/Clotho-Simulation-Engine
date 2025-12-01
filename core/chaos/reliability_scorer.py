"""
core/reliability_scorer.py

M5: Reliability Scoring System
Computes reliability score (0-100) based on state space coverage
"""
import math
from dataclasses import dataclass
from typing import Optional
from .coverage_tracker import CoverageStats


@dataclass
class ReliabilityScore:
    """
    Reliability score with confidence interval.
    
    Score interpretation:
    - 95-100: Excellent - High confidence in correctness
    - 85-94:  Good - Reasonable confidence
    - 70-84:  Fair - More testing recommended
    - < 70:   Poor - Insufficient coverage
    """
    score: float  # 0-100
    confidence_lower: float  # Lower bound of 95% CI
    confidence_upper: float  # Upper bound of 95% CI
    unique_states: int
    total_observations: int
    estimated_total_states: Optional[int]


class ReliabilityScorer:
    """
    Computes reliability score based on state space coverage.
    
    Philosophy: Unlike code coverage (lines executed), state coverage measures
    "how much of the possible system behavior have we explored?"
    
    Inspired by FoundationDB's approach: Run until no new states discovered
    for N consecutive simulations, then compute confidence score.
    """
    
    @staticmethod
    def compute_score(coverage: CoverageStats, num_simulations: int) -> ReliabilityScore:
        """
        Compute reliability score from coverage statistics.
        
        Args:
            coverage: CoverageStats from CoverageTracker
            num_simulations: Number of simulations run
            
        Returns:
            ReliabilityScore with score and confidence bounds
        """
        unique = coverage.unique_states
        total_obs = coverage.total_observations
        
        if unique == 0 or total_obs == 0:
            # No data - lowest score
            return ReliabilityScore(
                score=0.0,
                confidence_lower=0.0,
                confidence_upper=0.0,
                unique_states=0,
                total_observations=0,
                estimated_total_states=None
            )
        
        # Base score from coverage rate
        # High coverage rate = many unique states per observation
        base_score = min(coverage.coverage_rate * 100, 100.0)
        
        # Bonus for number of unique states discovered
        # More unique states = more thorough exploration
        state_bonus = min(math.log10(unique + 1) * 10, 30)
        
        # Bonus for number of simulations run
        # More simulations = higher confidence
        sim_bonus = min(math.log10(num_simulations + 1) * 5, 15)
        
        # Combined score (capped at 100)
        raw_score = base_score + state_bonus + sim_bonus
        score = min(raw_score, 100.0)
        
        # Compute confidence interval using Wilson score interval
        # This accounts for sample size - small samples have wider intervals
        confidence = 0.95
        z = 1.96  # z-score for 95% confidence
        
        # Proportion of unique states
        p = unique / total_obs
        
        # Wilson score interval formula
        denominator = 1 + (z**2) / total_obs
        center = (p + (z**2) / (2 * total_obs)) / denominator
        margin = (z * math.sqrt((p * (1 - p) / total_obs) + (z**2) / (4 * total_obs**2))) / denominator
        
        ci_lower = max(center - margin, 0.0) * 100
        ci_upper = min(center + margin, 1.0) * 100
        
        # Estimate total states using Good-Turing method
        estimated_total = ReliabilityScorer._estimate_total_states(unique, total_obs)
        
        return ReliabilityScore(
            score=round(score, 2),
            confidence_lower=round(ci_lower, 2),
            confidence_upper=round(ci_upper, 2),
            unique_states=unique,
            total_observations=total_obs,
            estimated_total_states=estimated_total
        )
    
    @staticmethod
    def _estimate_total_states(unique: int, total_obs: int) -> Optional[int]:
        """
        Estimate total possible states using Heaps' Law.
        
        Heaps' Law: V = K * N^β
        where V = unique states, N = observations, K and β are constants
        
        For distributed systems, β typically 0.4-0.6
        """
        if total_obs < 100:
            return None
        
        # Use β = 0.5 (conservative middle estimate)
        beta = 0.5
        k = unique / (total_obs ** beta)
        
        # Project to 10x current observations for 95% coverage estimate
        projected_obs = total_obs * 10
        estimated_total = int(k * (projected_obs ** beta))
        
        return max(estimated_total, unique)
    
    @staticmethod
    def interpret_score(score: float) -> str:
        """
        Human-readable interpretation of reliability score.
        
        Args:
            score: Reliability score (0-100)
            
        Returns:
            Interpretation string
        """
        if score >= 95:
            return "Excellent - High confidence in system correctness"
        elif score >= 85:
            return "Good - Reasonable confidence, minor gaps possible"
        elif score >= 70:
            return "Fair - More testing recommended"
        elif score >= 50:
            return "Poor - Significant gaps in coverage"
        else:
            return "Critical - Insufficient testing"
    
    @staticmethod
    def recommend_simulations(current_score: float, target_score: float, current_sims: int) -> int:
        """
        Recommend how many more simulations to run to reach target score.
        
        Args:
            current_score: Current reliability score
            target_score: Desired target score (e.g., 95)
            current_sims: Number of simulations already run
            
        Returns:
            Recommended additional simulations
        """
        if current_score >= target_score:
            return 0
        
        # Rough heuristic: logarithmic improvement
        # Each 2x increase in simulations adds ~5-10 points
        gap = target_score - current_score
        
        # Estimate: need 2^(gap/7) times current simulations
        multiplier = 2 ** (gap / 7)
        recommended_total = int(current_sims * multiplier)
        
        return max(recommended_total - current_sims, current_sims)  # At least double
