parent_dir=$(dirname "$(pwd)")
cd "$parent_dir" || exit
docker-compose -f docker-compose-2-flink.yaml up -d