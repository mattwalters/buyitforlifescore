import os
from dagster import DagsterInstance, DagsterRunStatus

def nuke_zombies():
    print("Connecting to Dagster SQLite Instance...")
    instance = DagsterInstance.get()
    
    zombie_statuses = [
        DagsterRunStatus.STARTING,
        DagsterRunStatus.STARTED,
        DagsterRunStatus.CANCELING,
    ]
    
    runs = instance.get_runs()
    zombies_killed = 0
    
    for run in runs:
        if run.status in zombie_statuses:
            print(f"Found zombie run: {run.run_id} (Status: {run.status.value})")
            # Forcefully mark the run as canceled in the database
            instance.report_run_canceled(run)
            instance.add_run_tags(
                run.run_id, 
                {"cancel_reason": "Forcefully wiped by zombie nuke script"}
            )
            zombies_killed += 1
            
    print(f"Done! Exorcised {zombies_killed} zombie runs from the database.")
    
if __name__ == "__main__":
    nuke_zombies()
