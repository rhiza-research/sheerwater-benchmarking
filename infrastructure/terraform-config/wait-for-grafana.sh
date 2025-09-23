until curl --output /dev/null --silent --head --fail $1; do
    printf '.'
    sleep 5
done
