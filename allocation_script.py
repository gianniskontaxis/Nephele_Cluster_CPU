from kubernetes import client, config

def get_cluster_metrics():
    config.load_kube_config()
    custom_api = client.CustomObjectsApi()

    clusters = []

    try:
        metrics = custom_api.list_cluster_custom_object(
            group="metrics.k8s.io",
            version="v1beta1",
            plural="nodes"
        )

        for metric in metrics.get('items', []):
            node_name = metric['metadata']['name']
            cpu = metric['usage'].get('cpu', "0n")
            memory = metric['usage'].get('memory', "0Ki")

            try:
                cpu_millicores = int(cpu.rstrip("n")) / 1_000_000  
                memory_kib = int(memory.rstrip("Ki"))  
            except ValueError:
                cpu_millicores = 0
                memory_kib = 0

            clusters.append({
                "id": node_name,
                "total_cpu": cpu_millicores,
                "total_memory": memory_kib,
                "available_cpu": cpu_millicores,
                "available_memory": memory_kib,
            })

        clusters = [cluster for cluster in clusters if "control-plane" not in cluster["id"]]

    except Exception as e:
        print(f"Failed to fetch cluster metrics: {str(e)}")

    return clusters


def print_cluster_metrics(clusters, message="Cluster Metrics:"):
    """
    Prints the cluster metrics in a readable format.
    """
    print(f"\n{message}")
    for cluster in clusters:
        print(
            f"Cluster {cluster['id']} - Total CPU: {cluster['total_cpu']:.2f}, "
            f"Available CPU: {cluster['available_cpu']:.2f} / Total Memory: {cluster['total_memory']} KiB, "
            f"Available Memory: {cluster['available_memory']} KiB"
        )


def allocate_services(clusters, services):
    def can_allocate(cluster, service):
        """
        Check if a service can be allocated to a given cluster.
        """
        return (
            cluster["available_cpu"] >= service["required_cpu"] and
            cluster["available_memory"] >= service["required_memory"]
        )

    def allocate_service(cluster, service):
        """
        Allocate resources for a service to a cluster.
        """
        cluster["available_cpu"] -= service["required_cpu"]
        cluster["available_memory"] -= service["required_memory"]

    allocations = []
    unallocated = []

    services = sorted(services, key=lambda s: (-s["required_cpu"], -s["required_memory"]))

    for service in services:
        allocated = False

        sorted_clusters = sorted(
            clusters,
            key=lambda c: (
                c["available_cpu"] - service["required_cpu"] if can_allocate(c, service) else float('inf'),
                c["available_memory"] - service["required_memory"] if can_allocate(c, service) else float('inf'),
                c["id"]  
            )
        )

        for cluster in sorted_clusters:
            if can_allocate(cluster, service):
                allocate_service(cluster, service)
                allocations.append({"service_id": service["id"], "cluster_id": cluster["id"]})
                allocated = True
                break

        if not allocated:
            unallocated.append(service)

    return allocations, unallocated


if __name__ == "__main__":
    clusters = get_cluster_metrics()

    if not clusters:
        print("No cluster metrics available. Exiting...")
        exit(1)

    print_cluster_metrics(clusters, message="Cluster Metrics Before Allocation:")
    
    services = [
        {"id": 1, "required_cpu": 10, "required_memory": 5120},
        {"id": 2, "required_cpu": 20, "required_memory": 10240},
        {"id": 3, "required_cpu": 15, "required_memory": 4096},
    ]

    allocations, unallocated = allocate_services(clusters, services)

    print("\nAllocations:")
    for allocation in allocations:
        print(f"Service {allocation['service_id']} -> Cluster {allocation['cluster_id']}")

    if unallocated:
        print("\nUnallocated Services:")
        for service in unallocated:
            print(f"Service {service['id']} (CPU: {service['required_cpu']}, Memory: {service['required_memory']})")
    else:
        print("\nAll services were successfully allocated.")

    print_cluster_metrics(clusters, message="Cluster Metrics After Allocation:")
