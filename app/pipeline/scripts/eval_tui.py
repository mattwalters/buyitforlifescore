import os
import sys
import subprocess
from pathlib import Path

def print_header(title):
    print(f"\n{'='*50}")
    print(f" {title.upper()}")
    print(f"{'='*50}")

def get_choice(prompt, valid_options, default=None):
    while True:
        choice = input(prompt).strip()
        if choice == "" and default is not None:
            return default
        if choice in valid_options:
            return choice
        print("Invalid selection. Try again.")

def get_model():
    print("\nAvailable Models:")
    print("1) gemini-2.5-flash-lite (Default)")
    print("2) gemini-3.1-flash-lite-preview")
    print("3) gemini-2.5-flash")
    print("4) gemini-3-flash-preview")
    print("5) gemini-3.1-pro-preview")
    print("6) Custom (Type your own)")
    
    choice = get_choice("Select a model [1-6] (Default 1): ", ["1", "2", "3", "4", "5", "6"], default="1")
    if choice == "1": return "gemini-2.5-flash-lite"
    if choice == "2": return "gemini-3.1-flash-lite-preview"
    if choice == "3": return "gemini-2.5-flash"
    if choice == "4": return "gemini-3-flash-preview"
    if choice == "5": return "gemini-3.1-pro-preview"
    if choice == "6": return input("Enter custom model string: ").strip()

def get_thinking(model):
    if 'gemini-3' in model:
        print("\nThinking Level:")
        print("1) None")
        print("2) Low")
        print("3) High")
        
        choice = get_choice("Select thinking level [1-3] (Default 2): ", ["1", "2", "3"], default="2")
        if choice == "1": return None
        if choice == "2": return "low"
        if choice == "3": return "high"
    else:
        print("\nThinking Budget (Tokens):")
        print("1) None")
        print("2) 1024 Tokens (Low)")
        print("3) 4096 Tokens (High)")
        print("4) Custom Token Allocation")
        
        # Default to 2 (1024) across the board. Silent intercept handles incompatible models downstream.
        choice = get_choice("Select thinking budget [1-4] (Default 2): ", ["1", "2", "3", "4"], default="2")
        if choice == "1": return None
        if choice == "2": return "1024"
        if choice == "3": return "4096"
        if choice == "4": return input("Enter token limit number (integer): ").strip()

def main():
    print_header("BIFL EVALUATION FRAMEWORK")
    print("1) Entity Discovery (Phase 1)")
    print("2) Entity Extraction (Phase 2)")
    print("3) Run Both Sequentially")
    
    suite = get_choice("Select benchmark suite [1-3] (Default 3): ", ["1", "2", "3"], default="3")
    model = get_model()
    thinking = get_thinking(model)
    
    verbose = get_choice("\nPrint detailed mismatch logs? (y/n) (Default y): ", ["y", "n", "Y", "N"], default="y").lower() == 'y'
        
    # Execution
    cwd = Path(__file__).parent.parent
    
    # We use the raw python executable inherently injected by 'uv run'
    base_cmd = ["python"]
    
    scripts_to_run = []
    if suite in ["1", "3"]:
        scripts_to_run.append("scripts/silver_entity_discovery_eval_runner.py")
    if suite in ["2", "3"]:
        scripts_to_run.append("scripts/silver_entity_extraction_eval_runner.py")
        
    for script_name in scripts_to_run:
        print_header(f"LAUNCHING: {script_name.split('/')[-1]}")
        cmd = base_cmd + [script_name, "--model", model]
        if thinking:
            cmd.extend(["--thinking", thinking])
        if verbose:
            cmd.append("--verbose")
            
        try:
            subprocess.run(cmd, cwd=cwd, check=True)
        except subprocess.CalledProcessError as e:
            print(f"\n❌ Error executing benchmark: {e}")
            sys.exit(1)
            
    print_header("EVALUATION COMPLETE")

if __name__ == "__main__":
    main()
