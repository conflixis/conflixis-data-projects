import os
import subprocess
import yaml
import sys
from dotenv import dotenv_values

def get_project_root():
    """Get the project root directory from the CONFIG.yaml file."""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'CONFIG.yaml')
    if not os.path.exists(config_path):
        print(f"Error: CONFIG.yaml not found at {config_path}")
        sys.exit(1)
        
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    project_dir = config.get('project_paths', {}).get('project_dir')
    if not project_dir or not os.path.isdir(project_dir):
        print(f"Error: 'project_dir' not configured or invalid in CONFIG.yaml: {project_dir}")
        sys.exit(1)
        
    return project_dir

def run_script(script_path, venv_python, env_vars):
    """Runs a python script using the specified virtual environment and environment variables."""
    try:
        print(f"--- Running script: {os.path.basename(script_path)} ---")
        
        # Combine the current environment with the loaded .env variables
        process_env = os.environ.copy()
        process_env.update(env_vars)

        process = subprocess.run(
            [venv_python, script_path],
            check=True,
            capture_output=True,
            text=True,
            env=process_env
        )
        print(process.stdout)
        print(f"--- Finished script: {os.path.basename(script_path)} ---")
        return True
    except subprocess.CalledProcessError as e:
        print(f"--- Error running script: {os.path.basename(script_path)} ---")
        print(f"Return code: {e.returncode}")
        print("\nSTDOUT:")
        print(e.stdout)
        print("\nSTDERR:")
        print(e.stderr)
        print(f"--- Script {os.path.basename(script_path)} failed ---")
        return False
    except FileNotFoundError:
        print(f"Error: Python interpreter not found at {venv_python}")
        print("Please ensure the virtual environment path is correct.")
        sys.exit(1)


def main():
    """Main function to orchestrate the analysis scripts."""
    project_root = get_project_root()
    scripts_dir = os.path.join(project_root, 'scripts')
    
    # Load environment variables from the .env file
    dotenv_path = os.path.join(project_root, '.env')
    if not os.path.exists(dotenv_path):
        print(f"Error: .env file not found at {dotenv_path}")
        sys.exit(1)
    
    env_vars = dotenv_values(dotenv_path)
    if 'GCP_SERVICE_ACCOUNT_KEY' not in env_vars or not env_vars['GCP_SERVICE_ACCOUNT_KEY']:
        print("Error: GCP_SERVICE_ACCOUNT_KEY not found or is empty in the .env file.")
        sys.exit(1)

    venv_python = "/home/incent/conflixis-data-projects/venv/bin/python"

    if not os.path.exists(venv_python):
        print(f"Error: Python interpreter not found at {venv_python}")
        print("Please check the venv path in run_full_analysis.py")
        sys.exit(1)

    analysis_scripts = [
        "01_upload_npi_list.py",
        "04_generate_report.py"
    ]

    print("=================================================")
    print("Starting Full Healthcare COI Analysis")
    print(f"Project Directory: {project_root}")
    print("=================================================")

    for script_name in analysis_scripts:
        script_path = os.path.join(scripts_dir, script_name)
        if not os.path.exists(script_path):
            print(f"Warning: Script not found, skipping: {script_name}")
            continue
        
        if not run_script(script_path, venv_python, env_vars):
            print(f"\nAnalysis halted due to an error in {script_name}.")
            sys.exit(1)
            
    print("=================================================")
    print("Full analysis completed successfully.")
    print("=================================================")

if __name__ == "__main__":
    main()