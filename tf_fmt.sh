#!/bin/bash

# format all terraform files in the current directory and subdirectories
# then apply tflint to fix or report any linting errors

# files to format/lint
files=$(find . -type f -name "*.tf" -not -path "*/.terraform/*")

for file in $files; do
  # format the file with the standard terraform fmt command
  terraform fmt $file;
  # then fix the formatting to a format that doesnt cause churn in PR's because the standard format is bad for reviewing code.
  # this forces the format `var = value` instead of `var[multiple spaces to align values]= value`
  sed -E -i.bak 's/[[:space:]]+=[[:space:]]+/ = /g' $file;
  # remove the backup file
  rm $file.bak;
done

# run tflint with our custom ruleset to fix or report any linting errors
tflint --fix --recursive

# validate specific terraform projects
tf_projects=(
  "infrastructure/terraform-config"
  "infrastructure/terraform-database"
)

for project in "${tf_projects[@]}"; do
  (
    cd "$project" 

    # check if the project has been initialized
    if [ ! -d ".terraform" ]; then
      # initialize the project without a backend (no state) just for validation
      echo "initializing $module"
      terraform init -backend=false
    fi

    echo "validating $project" 
    terraform validate
  )
done