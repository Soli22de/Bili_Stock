import os
import sys
import logging
import json

# Add parent directory to sys.path (Fix for module import)
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from core.ai_strategy import EvolutionaryTradingAI
import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def run_evolution():
    print("=== Starting AI Strategy Evolution ===")
    print("Target: Optimize DragonStrategy parameters")
    print("Population: 10 | Generations: 3 (Small scale for testing)")
    
    # 1. Initialize AI
    ai = EvolutionaryTradingAI()
    
    # 2. Run Evolution
    # 使用较小的种群和代数进行快速验证
    try:
        ai.evolve_strategies(population_size=10, generations=3)
    except Exception as e:
        print(f"❌ Evolution process failed: {e}")
        import traceback
        traceback.print_exc()
        return

    # 3. Save Best Strategy
    if ai.strategies:
        best_strategy = ai.strategies[0] # Assuming sorted or elite at top
        best_config = best_strategy.config.__dict__
        
        output_path = "data/best_strategy_params.json"
        with open(output_path, 'w') as f:
            json.dump(best_config, f, indent=4)
            
        print(f"\n✅ Evolution Complete!")
        print(f"Best Strategy Configuration saved to {output_path}")
        print("Top Parameters:")
        print(json.dumps(best_config, indent=4))
    else:
        print("⚠️  No strategies evolved.")

if __name__ == "__main__":
    run_evolution()
