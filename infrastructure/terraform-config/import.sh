#!/bin/bash

workspace=`terraform workspace show`

if [ "$workspace" == "default" ]; then
    # this is production, so the benchmarking org is the main org (id 1)
    terraform import grafana_organization.benchmarking 1
    #terraform import grafana_organization_preferences.light_preference_benchmarking 1
else
    # this is ephemeral, so the benchmarking org is the main org (id 1)
    terraform import grafana_organization.benchmarking 1
    terraform import grafana_organization_preferences.light_preference_benchmarking 1
fi
