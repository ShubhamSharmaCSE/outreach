"""
Load testing script for the sync service.
"""

import asyncio
import time
from typing import List
import random

import httpx
from src.models import SyncOperation, OperationType, RecordData


async def create_test_operation() -> dict:
    """Create a test sync operation."""
    
    operations = [OperationType.CREATE, OperationType.UPDATE, OperationType.DELETE]
    providers = ["salesforce_test", "hubspot_test", "pipedrive_test"]
    
    operation_data = {
        "operation": random.choice(operations).value,
        "provider": random.choice(providers),
        "priority": random.randint(1, 10)
    }
    
    if operation_data["operation"] in ["CREATE", "UPDATE"]:
        operation_data["record_data"] = {
            "data": {
                "name": f"Test User {random.randint(1000, 9999)}",
                "email": f"test{random.randint(1000, 9999)}@example.com",
                "phone": f"+1555{random.randint(1000000, 9999999)}"
            }
        }
    
    if operation_data["operation"] in ["UPDATE", "DELETE"]:
        operation_data["record_id"] = f"ext_{random.randint(100000, 999999)}"
    
    return operation_data


async def submit_operation(client: httpx.AsyncClient, operation_data: dict) -> dict:
    """Submit a single operation to the sync service."""
    
    try:
        response = await client.post("/sync", json=operation_data)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}


async def load_test_worker(
    worker_id: int,
    base_url: str,
    operations_per_worker: int,
    results: List[dict]
):
    """Worker function for load testing."""
    
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
        worker_results = []
        
        for i in range(operations_per_worker):
            start_time = time.time()
            
            # Create and submit operation
            operation_data = await create_test_operation()
            result = await submit_operation(client, operation_data)
            
            end_time = time.time()
            
            worker_results.append({
                "worker_id": worker_id,
                "operation_index": i,
                "duration": end_time - start_time,
                "success": "error" not in result,
                "result": result
            })
            
            # Brief pause to avoid overwhelming the service
            await asyncio.sleep(0.01)
        
        results.extend(worker_results)


async def run_load_test(
    base_url: str = "http://localhost:8000",
    total_operations: int = 1000,
    concurrent_workers: int = 10
):
    """Run load test against the sync service."""
    
    print(f"Starting load test:")
    print(f"  Base URL: {base_url}")
    print(f"  Total operations: {total_operations}")
    print(f"  Concurrent workers: {concurrent_workers}")
    print(f"  Operations per worker: {total_operations // concurrent_workers}")
    
    # Verify service is running
    try:
        async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
            response = await client.get("/health")
            if response.status_code != 200:
                print(f"Service health check failed: {response.status_code}")
                return
            print("Service health check passed")
    except Exception as e:
        print(f"Cannot connect to service: {e}")
        return
    
    # Setup test providers (simplified for load testing)
    test_providers = [
        {
            "name": "salesforce_test",
            "provider_type": "salesforce",
            "base_url": "https://mock.salesforce.com",
            "rate_limit_per_minute": 5000,
            "burst_limit": 200,
            "auth_config": {
                "auth_type": "oauth2",
                "credentials": {
                    "client_id": "test_client",
                    "client_secret": "test_secret"
                },
                "token_url": "https://mock.salesforce.com/oauth2/token"
            }
        },
        {
            "name": "hubspot_test",
            "provider_type": "hubspot",
            "base_url": "https://mock.hubspot.com",
            "rate_limit_per_minute": 10000,
            "burst_limit": 300,
            "auth_config": {
                "auth_type": "api_key",
                "credentials": {
                    "api_key": "test_api_key"
                }
            }
        },
        {
            "name": "pipedrive_test",
            "provider_type": "pipedrive",
            "base_url": "https://mock.pipedrive.com",
            "rate_limit_per_minute": 2000,
            "burst_limit": 100,
            "auth_config": {
                "auth_type": "api_key",
                "credentials": {
                    "api_key": "test_api_key"
                }
            }
        }
    ]
    
    # Add test providers
    print("Setting up test providers...")
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
        for provider in test_providers:
            try:
                response = await client.post("/providers", json=provider)
                if response.status_code == 200:
                    print(f"  ✓ Added provider: {provider['name']}")
                else:
                    print(f"  ✗ Failed to add provider {provider['name']}: {response.status_code}")
            except Exception as e:
                print(f"  ✗ Error adding provider {provider['name']}: {e}")
    
    # Run load test
    print(f"\nStarting load test with {concurrent_workers} workers...")
    start_time = time.time()
    
    results = []
    operations_per_worker = total_operations // concurrent_workers
    
    # Create worker tasks
    tasks = []
    for worker_id in range(concurrent_workers):
        task = asyncio.create_task(
            load_test_worker(worker_id, base_url, operations_per_worker, results)
        )
        tasks.append(task)
    
    # Wait for all workers to complete
    await asyncio.gather(*tasks)
    
    end_time = time.time()
    total_duration = end_time - start_time
    
    # Analyze results
    successful_ops = sum(1 for r in results if r["success"])
    failed_ops = len(results) - successful_ops
    
    if results:
        durations = [r["duration"] for r in results]
        avg_duration = sum(durations) / len(durations)
        min_duration = min(durations)
        max_duration = max(durations)
        
        # Calculate percentiles
        sorted_durations = sorted(durations)
        p50 = sorted_durations[len(sorted_durations) // 2]
        p95 = sorted_durations[int(len(sorted_durations) * 0.95)]
        p99 = sorted_durations[int(len(sorted_durations) * 0.99)]
    else:
        avg_duration = min_duration = max_duration = p50 = p95 = p99 = 0
    
    # Print results
    print(f"\nLoad Test Results:")
    print(f"  Total duration: {total_duration:.2f} seconds")
    print(f"  Operations completed: {len(results)}")
    print(f"  Successful operations: {successful_ops}")
    print(f"  Failed operations: {failed_ops}")
    print(f"  Success rate: {(successful_ops / len(results) * 100):.1f}%" if results else "0%")
    print(f"  Operations per second: {len(results) / total_duration:.1f}")
    print(f"\nResponse Times:")
    print(f"  Average: {avg_duration:.3f}s")
    print(f"  Min: {min_duration:.3f}s")
    print(f"  Max: {max_duration:.3f}s")
    print(f"  P50: {p50:.3f}s")
    print(f"  P95: {p95:.3f}s")
    print(f"  P99: {p99:.3f}s")
    
    # Show failed operations
    if failed_ops > 0:
        print(f"\nFailed Operations (showing first 5):")
        failed_results = [r for r in results if not r["success"]][:5]
        for i, result in enumerate(failed_results, 1):
            print(f"  {i}. Worker {result['worker_id']}: {result['result'].get('error', 'Unknown error')}")


if __name__ == "__main__":
    # Run load test
    asyncio.run(run_load_test(
        base_url="http://localhost:8000",
        total_operations=1000,
        concurrent_workers=10
    ))
