#!/usr/bin/env python3
# for each dashboard in the dashboards folder, build the dashboard and save it to the build folder

import os
import sys
import time
import json
import glob
import shutil
import subprocess
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from grafana_foundation_sdk.cog.encoder import JSONEncoder

def run_command(command):
    """Run a shell command and return output"""
    process = subprocess.run(command, shell=True, capture_output=True, text=True)
    if process.returncode != 0:
        print(f"Error running command: {command}")
        print(f"Error: {process.stderr}")
        raise Exception(f"Command failed with exit code {process.returncode}")
    return process.stdout.strip()

# def deploy_to_grafana(build_dir, config_file):
#     """Deploy dashboards to Grafana using grafanactl"""
#     try:
#         # Push all dashboards from the build directory
#         run_command(f"grafanactl --config {config_file} resources push --path {build_dir}")
#         print(f"Successfully deployed dashboards from {build_dir}")
#         return True
#     except Exception as e:
#         print(f"Error deploying dashboards: {e}")
#         return False

class DashboardHandler(FileSystemEventHandler):
    def __init__(self, source_dir, build_dir, config_file, datasource_config_file):
        self.last_build_time = {}
        self.debounce_interval = 1.0  # seconds
        self.source_dir = source_dir
        self.build_dir = build_dir
        self.config_file = config_file
        self.datasource_config_file = datasource_config_file

    def on_modified(self, event):
        if event.is_directory:
            return
        
        if not event.src_path.endswith('.py'):
            return
            
        # Debounce to prevent multiple builds for the same file
        current_time = time.time()
        last_time = self.last_build_time.get(event.src_path, 0)
        if current_time - last_time < self.debounce_interval:
            return
            
        self.last_build_time[event.src_path] = current_time
        self.build_dashboard(event.src_path)

    def build_dashboard(self, src_path):
        """Build a single dashboard from source file"""
        try:
            # Get dashboard name from filename
            dashboard_name = Path(src_path).stem.replace('_', ' ').title()
            dashboard_uid = Path(src_path).stem.lower()
            
            # Import the dashboard module dynamically
            sys.path.insert(0, str(Path(src_path).parent))
            module_name = Path(src_path).stem
            if module_name in sys.modules:
                del sys.modules[module_name]
            dashboard_module = __import__(module_name)
            sys.path.pop(0)
            
            # Build dashboard using the module's build function
            dashboard = dashboard_module.build_dashboard(self.datasource_config_file)
            
            # Convert to JSON using Foundation SDK encoder
            encoder = JSONEncoder(sort_keys=True)
            dashboard_json = encoder.encode(dashboard)
            dashboard_dict = json.loads(dashboard_json)
            
            # Remove null options fields
            def remove_null_options(obj):
                if isinstance(obj, dict):
                    if 'options' in obj and obj['options'] is None:
                        del obj['options']
                    for value in obj.values():
                        remove_null_options(value)
                elif isinstance(obj, list):
                    for item in obj:
                        remove_null_options(item)

            remove_null_options(dashboard_dict)
            
            # Calculate relative path from source directory and create corresponding build path
            rel_path = os.path.relpath(src_path, self.source_dir)
            # Replace .py extension with .json
            rel_path = os.path.splitext(rel_path)[0] + '.json'
            build_path = os.path.join(self.build_dir, rel_path)
            
            # Create destination directory if it doesn't exist
            os.makedirs(os.path.dirname(build_path), exist_ok=True)
            # resource_manifest = {
            #     "apiVersion": "dashboard.grafana.app/v1beta1",
            #     "kind": "Dashboard",
            #     "metadata": {
            #         "name": dashboard_uid
            #     },
            #     "spec": dashboard_dict
            # }
            
            with open(build_path, 'w') as f:
                json.dump(dashboard_dict, f, indent=2, ensure_ascii=False)
                
            print(f"Built dashboard: {dashboard_name} -> {build_path}")
            
            # Deploy all dashboards using grafanactl
            #deploy_to_grafana(self.build_dir, self.config_file)
            
        except Exception as e:
            print(f"Error building dashboard {src_path}: {e}")

    def build_all_dashboards(self):
        """Build all dashboards in the dashboards directory and subdirectories"""
        # Find all Python files recursively
        dashboard_files = glob.glob(f'{self.source_dir}/**/*.py', recursive=True)
        
        print(f"Building {len(dashboard_files)} dashboards...")
        for dashboard_file in dashboard_files:
            self.build_dashboard(dashboard_file)

        # Find all JSON files recursively
        dashboard_json_files = glob.glob(f'{self.source_dir}/**/*.json', recursive=True)
        print(f"Copying {len(dashboard_json_files)} dashboard JSON files...")
        for dashboard_json_file in dashboard_json_files:
            # Calculate relative path from source directory
            rel_path = os.path.relpath(dashboard_json_file, self.source_dir)
            # Create corresponding path in build directory
            dest_path = os.path.join(self.build_dir, rel_path)
            # Create destination directory if it doesn't exist
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            # Copy the file
            shutil.copy(dashboard_json_file, dest_path)

    def watch_dashboards(self):
        """Watch for changes in dashboard files and rebuild them"""
        # first build all dashboards
        self.build_all_dashboards()

        # then watch for changes
        observer = Observer()
        observer.schedule(self, self.source_dir, recursive=True)
        observer.start()
        
        print("Watching for dashboard changes... Press Ctrl+C to stop")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()

if __name__ == "__main__":
    import argparse

    source_dir = os.path.join(os.path.dirname(__file__), "src")
    build_dir = os.path.join(os.path.dirname(__file__), "build")
    config_file = os.path.join(os.path.dirname(__file__), "config.yaml")

    datasource_config_file = os.path.join(os.path.dirname(__file__), "datasource_config.json")


    parser = argparse.ArgumentParser(description="Build and watch Grafana dashboards")
    parser.add_argument("action", choices=["build", "watch"],
                      help="Action to perform: build (one-time build) or watch (continuous build)")
    
    parser.add_argument("--source-dir", type=str, default=source_dir,
                      help="Source directory containing dashboard files")
    parser.add_argument("--build-dir", type=str, default=build_dir,
                      help="Build directory for dashboard files")
    parser.add_argument("--config", type=str, default=config_file,
                      help="Config file for grafanactl")
    parser.add_argument("--datasource-config", type=str, default=datasource_config_file,
                      help="Datasource config file")
    args = parser.parse_args()

    # Create build directory if it doesn't exist
    os.makedirs(args.build_dir, exist_ok=True)

    handler = DashboardHandler(args.source_dir, args.build_dir, args.config, args.datasource_config)
    
    if args.action == "build":
        # Build all dashboards once
        handler.build_all_dashboards()
    elif args.action == "watch":
        # Build all dashboards, then watch for changes
        handler.watch_dashboards()
    
