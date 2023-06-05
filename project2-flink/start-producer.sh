parent_dir=$(dirname "$(pwd)")
cd "$parent_dir" || exit
cd project2 || exit
python producer-flink.py