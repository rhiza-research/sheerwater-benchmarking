#!/usr/bin/env python3
"""
Simple PR number detection using only GitHub's built-in mechanisms.
No external dependencies, API calls, or naming conventions.
"""

import subprocess
import re
import sys
from typing import Optional


def run_git_command(cmd: list[str]) -> str:
    """Run a git command and return the output."""
    try:
        result = subprocess.run(
            ["git"] + cmd, 
            capture_output=True, 
            text=True, 
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return ""


def extract_pr_from_github_merge_commits() -> Optional[int]:
    """Extract PR number from GitHub's merge commit messages."""
    try:
        # Get recent commits, focusing on merge commits from GitHub
        output = run_git_command(["log", "--oneline", "-20", "--merges"])
        if not output:
            # Fallback to any recent commits
            output = run_git_command(["log", "--oneline", "-10"])
        
        lines = output.split('\n')
        for line in lines:
            # Look for GitHub's specific merge commit pattern
            if "Merge pull request #" in line:
                match = re.search(r"Merge pull request #(\d+)", line)
                if match:
                    return int(match.group(1))
            
            # Look for GitHub's automatic PR references in commit messages
            match = re.search(r"\(#(\d+)\)", line)
            if match:
                return int(match.group(1))
    except:
        pass
    
    return None


def extract_pr_from_remote_refs() -> Optional[int]:
    """Extract PR number from GitHub's remote refs."""
    try:
        # Get all remote refs
        output = run_git_command(["ls-remote", "origin"])
        
        # Get current commit SHA
        current_sha = run_git_command(["rev-parse", "HEAD"])
        if not current_sha:
            return None
        
        lines = output.split('\n')
        
        # First, check for exact match
        for line in lines:
            if current_sha in line and "refs/pull/" in line:
                match = re.search(r"refs/pull/(\d+)/", line)
                if match:
                    return int(match.group(1))
        
        # If no exact match, check if current commit is descended from any PR
        pr_refs = {}
        for line in lines:
            if "refs/pull/" in line and "/head" in line:
                match = re.search(r"refs/pull/(\d+)/head", line)
                if match:
                    pr_number = int(match.group(1))
                    pr_sha = line.split()[0]
                    pr_refs[pr_number] = pr_sha
        
        # Find the most recent PR ancestor (closest to current commit)
        closest_pr = None
        closest_distance = float('inf')
        
        for pr_number, pr_sha in pr_refs.items():
            # Check if pr_sha is an ancestor of current commit using merge-base
            merge_base = run_git_command(["merge-base", pr_sha, current_sha])
            if merge_base == pr_sha:  # If merge-base equals pr_sha, then pr_sha is an ancestor
                # Calculate distance from PR to current commit
                distance_output = run_git_command(["rev-list", "--count", f"{pr_sha}..{current_sha}"])
                if distance_output and distance_output.isdigit():
                    distance = int(distance_output)
                    if distance < closest_distance:
                        closest_distance = distance
                        closest_pr = pr_number
        
        return closest_pr
                
    except:
        pass
    
    return None


def extract_pr_from_reflog() -> Optional[int]:
    """Extract PR number from git reflog if it contains GitHub refs."""
    try:
        # Look for GitHub's pull refs in reflog
        output = run_git_command(["reflog", "--oneline", "-20"])
        
        lines = output.split('\n')
        for line in lines:
            # Only look for GitHub's official refs/pull pattern
            match = re.search(r"refs/pull/(\d+)/", line)
            if match:
                return int(match.group(1))
    except:
        pass
    
    return None


def get_pr_number() -> Optional[int]:
    """
    Try to get PR number using only GitHub's built-in mechanisms.
    Returns the first successful match or None.
    """
    
    # Method 1: Extract from GitHub's remote refs (most reliable)
    pr_number = extract_pr_from_remote_refs()
    if pr_number:
        return pr_number
    
    # Method 2: Extract from GitHub's merge commit messages
    pr_number = extract_pr_from_github_merge_commits()
    if pr_number:
        return pr_number
    
    # Method 3: Extract from git reflog (GitHub refs only)
    pr_number = extract_pr_from_reflog()
    if pr_number:
        return pr_number
    
    return None

def generate_config_yaml(pr_number: int) -> str:
    """Generate a config.yaml file with the PR number."""
    return f"""
contexts:
    ephemeral:
        grafana:
            server: https://grafana-pr-{pr_number}.dev.shared.rhizaresearch.org
            org-id: 1
current-context: ephemeral
"""


def main():
    """Main function."""
    pr_number = get_pr_number()
    
    if pr_number:
        #print(pr_number)
        config_yaml = generate_config_yaml(pr_number)
        with open("config.yaml", "w") as f:
            f.write(config_yaml)
        print(f"Wrote config.yaml for PR {pr_number}")
        sys.exit(0)
    else:
        print("Could not determine PR number from GitHub's built-in mechanisms", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main() 