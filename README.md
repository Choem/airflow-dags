# airflow-dags

Tools needed for building and pushing DAGS to remote:
- Git
- Docker
- Python

Because our Airflow instance is running on k8s we want to utilize the resources as efficient as possible.
That is why you need to be sure that the DAG utilizes the KubernetesPodOperator so it spins up a seperate pod in k8s when executed and shutsdown when completed.

# Build and push Docker image
The Docker image needs to be available for a k8s cluster to be pulled and used. 
This is done by building and pushing it to a registry.
This can be a local, public or private Docker registry.

1. Build the Docker image which will get used by the KubernetesPodOperator, command: ```docker build -t <registry>/<image>:<tag> . ```

2. Push the Docker image to a designated registry, command: ```docker push <registry>/<image>:<tag>```

# Update your DAG
This is an important step as you want your DAG to use the correct Docker image, the one that you just built.
Make sure to increment the tag!

3. Don't forget to update the image tag in your DAG ```dags/<dag>```

# Push code to remote
Last but not least don't forget to push your changes to remote. 
The DAG will eventually get synced to the k8s cluster and be executed.

1. git add .

2. git commit -m ```<message>```

3. git push ```<branch>```
