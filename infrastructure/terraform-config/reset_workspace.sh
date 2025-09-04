#!/bin/bash

#workspace=$1
workspace=`terraform workspace show`

# fail if the workspace is default
if [ "$workspace" == "default" ]; then
    echo "Cannot delete the default workspace"
    exit 1
fi

# if [ -z "$workspace" ]; then
#     echo "No workspace provided"
#     echo "Usage: $0 <workspace>"
#     echo "Available workspaces:"
#     terraform workspace list
#     exit 1
# fi

echo "Are you sure you want to delete the workspace $workspace? (y/n)"
read confirm
if [ "$confirm" != "y" ]; then
    echo "Aborting..."
    exit 1
fi

# Delete all resources in the workspace
terraform workspace select default
terraform workspace delete -force $workspace
terraform workspace new $workspace