# Create GCP service account
sa_file_path=pocz-389704.json
kubectl kubectl create secret generic gcp-sa --from-file=${sa_file_path}