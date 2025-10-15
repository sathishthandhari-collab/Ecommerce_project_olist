# Python Interview Questions for Analytics Engineers

## 50 Comprehensive Python Questions for Analytics Engineering

### Data Structures and Core Concepts (1-10)

**Q1: Explain Python's data structures and when to use each in Analytics Engineering contexts.**

**Answer**:
**Core Data Structures for Analytics Engineering**:

```python
from collections import defaultdict, deque, Counter, namedtuple
from typing import Dict, List, Set, Tuple, Optional, Union
import heapq

# 1. Lists - Ordered, mutable sequences
customer_ids = [1001, 1002, 1003, 1004]
# Use case: Processing records in order, building data batches

def batch_process_customers(customer_list: List[int], batch_size: int = 100):
    """Process customers in batches for API calls"""
    for i in range(0, len(customer_list), batch_size):
        batch = customer_list[i:i + batch_size]
        # Process batch via API
        yield batch

# 2. Dictionaries - Key-value mappings, O(1) average lookup
customer_metrics = {
    'customer_1001': {'ltv': 1250.50, 'orders': 15, 'segment': 'high_value'},
    'customer_1002': {'ltv': 450.25, 'orders': 5, 'segment': 'standard'}
}
# Use case: Fast lookups, configuration, data transformation mappings

def enrich_orders_with_customer_data(orders: List[Dict], customer_data: Dict[str, Dict]) -> List[Dict]:
    """Enrich order data with customer information"""
    enriched_orders = []
    for order in orders:
        customer_id = order.get('customer_id')
        if customer_id in customer_data:
            order.update(customer_data[customer_id])
        enriched_orders.append(order)
    return enriched_orders

# 3. Sets - Unique elements, O(1) average membership testing
processed_order_ids = {1001, 1002, 1003, 1005}
new_order_ids = {1004, 1005, 1006, 1007}

# Find new orders to process
unprocessed_orders = new_order_ids - processed_order_ids
# Use case: Deduplication, membership testing, set operations

def identify_customer_segments_overlap(segment_a: Set[str], segment_b: Set[str]) -> Dict[str, Set[str]]:
    """Analyze customer segment overlaps"""
    return {
        'intersection': segment_a & segment_b,
        'segment_a_only': segment_a - segment_b,
        'segment_b_only': segment_b - segment_a,
        'union': segment_a | segment_b
    }

# 4. Tuples - Immutable sequences, hashable
coordinate = (40.7128, -74.0060)  # lat, lng for NYC
# Use case: Immutable data, dictionary keys, function returns

def calculate_distance_matrix(locations: List[Tuple[float, float]]) -> Dict[Tuple[Tuple[float, float], Tuple[float, float]], float]:
    """Calculate distance matrix between locations"""
    distances = {}
    for i, loc1 in enumerate(locations):
        for j, loc2 in enumerate(locations):
            if i != j:
                # Calculate distance (simplified)
                distance = ((loc1[0] - loc2[0])**2 + (loc1[1] - loc2[1])**2)**0.5
                distances[(loc1, loc2)] = distance
    return distances

# 5. Advanced Collections - Specialized data structures
from collections import defaultdict, deque, Counter, namedtuple

# defaultdict - Automatic default values
sales_by_region = defaultdict(list)
for sale in sales_data:
    sales_by_region[sale['region']].append(sale['amount'])

# Counter - Element counting
product_popularity = Counter(order['product_id'] for order in orders)
top_products = product_popularity.most_common(10)

# deque - Double-ended queue for sliding windows
class SlidingWindowAnalyzer:
    def __init__(self, window_size: int):
        self.window = deque(maxlen=window_size)
        self.sum = 0
    
    def add_value(self, value: float) -> float:
        """Add value and return current moving average"""
        if len(self.window) == self.window.max_len:
            # Remove oldest value from sum
            self.sum -= self.window[0]
        
        self.window.append(value)
        self.sum += value
        return self.sum / len(self.window)

# namedtuple - Lightweight object-like structures
Customer = namedtuple('Customer', ['id', 'name', 'email', 'segment', 'ltv'])

def create_customer_objects(customer_data: List[Dict]) -> List[Customer]:
    """Convert dictionaries to structured customer objects"""
    return [Customer(**data) for data in customer_data]

# 6. Priority Queue using heapq for analytics
class MetricsPriorityQueue:
    """Priority queue for processing high-value customers first"""
    
    def __init__(self):
        self._heap = []
        self._index = 0
    
    def push(self, customer_id: str, priority: float, data: Dict):
        """Add customer with priority (lower number = higher priority)"""
        # Negate priority for max-heap behavior
        heapq.heappush(self._heap, (-priority, self._index, customer_id, data))
        self._index += 1
    
    def pop(self) -> Tuple[str, Dict]:
        """Get highest priority customer"""
        if self._heap:
            _, _, customer_id, data = heapq.heappop(self._heap)
            return customer_id, data
        return None, None

# Usage example
priority_queue = MetricsPriorityQueue()
for customer in customers:
    priority_queue.push(
        customer['id'], 
        customer['ltv'],  # Higher LTV = higher priority
        customer
    )
```

**Performance Characteristics**:
- **Lists**: O(1) append, O(n) insert/delete at beginning
- **Dicts**: O(1) average access, O(n) worst case
- **Sets**: O(1) average membership test, O(n) worst case
- **Tuples**: O(1) access, immutable (memory efficient)

---

**Q2: How do you implement efficient data processing patterns in Python for large-scale analytics workloads?**

**Answer**:
**Memory-Efficient Processing Patterns**:

```python
import itertools
from typing import Generator, Iterator, Callable, Any
import csv
import json
from contextlib import contextmanager
import gc
import psutil
import time

# 1. Generator-based processing for memory efficiency
def process_large_dataset(filename: str) -> Generator[Dict, None, None]:
    """Process large CSV files without loading entire dataset into memory"""
    with open(filename, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Transform data on-the-fly
            yield {
                'customer_id': int(row['customer_id']),
                'order_amount': float(row['order_amount']),
                'order_date': row['order_date'],
                'processed_at': time.time()
            }

def batch_generator(data_generator: Generator, batch_size: int = 1000) -> Generator[List[Dict], None, None]:
    """Convert generator into batches for efficient API calls"""
    iterator = iter(data_generator)
    while True:
        batch = list(itertools.islice(iterator, batch_size))
        if not batch:
            break
        yield batch

# Usage: Memory-efficient large file processing
filename = '/path/to/large_dataset.csv'
for batch in batch_generator(process_large_dataset(filename), batch_size=500):
    # Process batch - only 500 records in memory at once
    process_batch_to_warehouse(batch)

# 2. Streaming aggregations for real-time analytics
class StreamingAggregator:
    """Streaming aggregation with sliding windows"""
    
    def __init__(self, window_size_minutes: int = 60):
        self.window_size = window_size_minutes * 60  # Convert to seconds
        self.data_points = deque()
        self.metrics = {
            'count': 0,
            'sum': 0.0,
            'min': float('inf'),
            'max': float('-inf')
        }
    
    def add_data_point(self, timestamp: float, value: float):
        """Add new data point and maintain sliding window"""
        # Remove old data points outside window
        cutoff_time = timestamp - self.window_size
        while self.data_points and self.data_points[0][0] < cutoff_time:
            old_timestamp, old_value = self.data_points.popleft()
            self._remove_from_metrics(old_value)
        
        # Add new data point
        self.data_points.append((timestamp, value))
        self._add_to_metrics(value)
    
    def _add_to_metrics(self, value: float):
        """Update metrics with new value"""
        self.metrics['count'] += 1
        self.metrics['sum'] += value
        self.metrics['min'] = min(self.metrics['min'], value)
        self.metrics['max'] = max(self.metrics['max'], value)
    
    def _remove_from_metrics(self, value: float):
        """Remove value from metrics (approximate for min/max)"""
        self.metrics['count'] -= 1
        self.metrics['sum'] -= value
        # Note: Min/max would need full recalculation for exact values
        
    def get_current_metrics(self) -> Dict[str, float]:
        """Get current window metrics"""
        if self.metrics['count'] == 0:
            return {'count': 0, 'sum': 0, 'avg': 0, 'min': 0, 'max': 0}
        
        return {
            'count': self.metrics['count'],
            'sum': self.metrics['sum'],
            'avg': self.metrics['sum'] / self.metrics['count'],
            'min': self.metrics['min'],
            'max': self.metrics['max']
        }

# 3. Memory monitoring and management
@contextmanager
def memory_monitor(operation_name: str):
    """Context manager for monitoring memory usage"""
    process = psutil.Process()
    initial_memory = process.memory_info().rss / 1024 / 1024  # MB
    
    print(f"Starting {operation_name} - Initial memory: {initial_memory:.2f} MB")
    
    try:
        yield
    finally:
        gc.collect()  # Force garbage collection
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_diff = final_memory - initial_memory
        
        print(f"Completed {operation_name} - Final memory: {final_memory:.2f} MB")
        print(f"Memory change: {memory_diff:+.2f} MB")

# Usage
with memory_monitor("Large dataset processing"):
    results = []
    for batch in batch_generator(process_large_dataset('large_file.csv')):
        processed_batch = [transform_record(record) for record in batch]
        results.extend(processed_batch)
        
        # Periodically clear intermediate results
        if len(results) > 10000:
            write_to_warehouse(results)
            results.clear()
            gc.collect()

# 4. Efficient data transformations using itertools
def customer_cohort_analysis(customers: Iterator[Dict]) -> Dict[str, List[Dict]]:
    """Efficient cohort analysis using itertools"""
    
    # Group customers by signup month
    def get_signup_month(customer):
        return customer['signup_date'][:7]  # YYYY-MM format
    
    # Sort customers by signup date for groupby
    sorted_customers = sorted(customers, key=get_signup_month)
    
    cohorts = {}
    for month, group in itertools.groupby(sorted_customers, key=get_signup_month):
        cohort_customers = list(group)
        
        # Calculate cohort metrics efficiently
        cohorts[month] = {
            'customer_count': len(cohort_customers),
            'total_ltv': sum(c.get('ltv', 0) for c in cohort_customers),
            'avg_ltv': sum(c.get('ltv', 0) for c in cohort_customers) / len(cohort_customers),
            'customers': cohort_customers
        }
    
    return cohorts

# 5. Parallel processing patterns
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from multiprocessing import Pool
import threading

class ParallelDataProcessor:
    """Parallel processing for CPU and I/O intensive tasks"""
    
    def __init__(self, max_workers: Optional[int] = None):
        self.max_workers = max_workers or min(32, (psutil.cpu_count() or 1) + 4)
    
    def process_cpu_intensive(self, data_chunks: List[List[Dict]], 
                             processor_func: Callable) -> List[Any]:
        """Use ProcessPoolExecutor for CPU-intensive tasks"""
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(processor_func, chunk) for chunk in data_chunks]
            
            results = []
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as exc:
                    print(f'Data chunk processing generated an exception: {exc}')
            
            return results
    
    def process_io_intensive(self, api_endpoints: List[str], 
                            fetcher_func: Callable) -> List[Any]:
        """Use ThreadPoolExecutor for I/O-intensive tasks"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(fetcher_func, endpoint) for endpoint in api_endpoints]
            
            results = []
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as exc:
                    print(f'API call generated an exception: {exc}')
            
            return results

# Example usage
def calculate_customer_metrics(customer_batch: List[Dict]) -> Dict:
    """CPU-intensive customer metric calculations"""
    metrics = {
        'processed_customers': len(customer_batch),
        'total_ltv': 0,
        'high_value_customers': 0
    }
    
    for customer in customer_batch:
        # Complex LTV calculation
        ltv = calculate_complex_ltv(customer)
        metrics['total_ltv'] += ltv
        
        if ltv > 1000:
            metrics['high_value_customers'] += 1
    
    return metrics

def fetch_external_data(api_endpoint: str) -> Dict:
    """I/O-intensive external data fetching"""
    import requests
    try:
        response = requests.get(api_endpoint, timeout=30)
        return response.json()
    except Exception as e:
        return {'error': str(e), 'endpoint': api_endpoint}

# Process data in parallel
processor = ParallelDataProcessor(max_workers=8)

# CPU-intensive processing
customer_chunks = [customers[i:i+1000] for i in range(0, len(customers), 1000)]
cpu_results = processor.process_cpu_intensive(customer_chunks, calculate_customer_metrics)

# I/O-intensive processing
api_endpoints = ['https://api.example.com/data/segment1', 
                'https://api.example.com/data/segment2']
io_results = processor.process_io_intensive(api_endpoints, fetch_external_data)

# 6. Caching and memoization for expensive computations
from functools import lru_cache, wraps
import hashlib
import pickle
import os

def disk_cache(cache_dir: str = './cache', ttl_seconds: int = 3600):
    """Decorator for persistent disk-based caching"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create cache key from function arguments
            key_data = pickle.dumps((func.__name__, args, kwargs))
            cache_key = hashlib.md5(key_data).hexdigest()
            cache_file = os.path.join(cache_dir, f"{cache_key}.cache")
            
            # Check if cache exists and is fresh
            if os.path.exists(cache_file):
                file_age = time.time() - os.path.getmtime(cache_file)
                if file_age < ttl_seconds:
                    with open(cache_file, 'rb') as f:
                        return pickle.load(f)
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            
            os.makedirs(cache_dir, exist_ok=True)
            with open(cache_file, 'wb') as f:
                pickle.dump(result, f)
            
            return result
        return wrapper
    return decorator

@lru_cache(maxsize=128)
def calculate_complex_customer_score(customer_id: int, 
                                   segment: str, 
                                   purchase_history: Tuple[float, ...]) -> float:
    """Expensive calculation with memory caching"""
    # Complex scoring algorithm
    base_score = sum(purchase_history) / len(purchase_history) if purchase_history else 0
    segment_multiplier = {'premium': 1.5, 'standard': 1.0, 'basic': 0.8}.get(segment, 1.0)
    
    # Simulate expensive computation
    time.sleep(0.1)  # Represents complex calculation
    
    return base_score * segment_multiplier

@disk_cache(cache_dir='./ml_cache', ttl_seconds=86400)  # 24 hours
def train_customer_segmentation_model(features_data: List[Dict]) -> Dict:
    """Expensive ML model training with disk caching"""
    from sklearn.cluster import KMeans
    import numpy as np
    
    # Convert to feature matrix
    features = np.array([[f['ltv'], f['frequency'], f['recency']] 
                        for f in features_data])
    
    # Train model
    model = KMeans(n_clusters=5, random_state=42)
    model.fit(features)
    
    return {
        'model_params': model.get_params(),
        'cluster_centers': model.cluster_centers_.tolist(),
        'training_size': len(features_data)
    }
```

**Key Efficiency Principles**:
- **Generator-based processing**: Process data in streams, not batches
- **Memory monitoring**: Track and optimize memory usage proactively  
- **Parallel execution**: Use appropriate parallelism for CPU vs I/O tasks
- **Intelligent caching**: Cache expensive computations with appropriate TTL
- **Sliding windows**: Efficient real-time aggregations without storing all data

---

### Object-Oriented Programming and Design Patterns (11-20)

**Q3: Design object-oriented systems for Analytics Engineering using SOLID principles and appropriate design patterns.**

**Answer**:
**SOLID Principles in Analytics Engineering**:

```python
from abc import ABC, abstractmethod
from typing import Protocol, List, Dict, Any, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
import logging
from enum import Enum

# 1. Single Responsibility Principle (SRP)
# Each class has a single, well-defined responsibility

class DataValidator:
    """Responsible only for data validation"""
    
    def __init__(self, validation_rules: Dict[str, Any]):
        self.validation_rules = validation_rules
    
    def validate_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate a single record against rules"""
        errors = []
        
        for field, rules in self.validation_rules.items():
            if field not in record:
                if rules.get('required', False):
                    errors.append(f"Missing required field: {field}")
                continue
            
            value = record[field]
            
            # Type validation
            expected_type = rules.get('type')
            if expected_type and not isinstance(value, expected_type):
                errors.append(f"Field {field} has wrong type: expected {expected_type.__name__}")
            
            # Range validation
            if 'min_value' in rules and value < rules['min_value']:
                errors.append(f"Field {field} below minimum: {rules['min_value']}")
            
            if 'max_value' in rules and value > rules['max_value']:
                errors.append(f"Field {field} above maximum: {rules['max_value']}")
        
        return len(errors) == 0, errors

class DataTransformer:
    """Responsible only for data transformation"""
    
    def __init__(self, transformation_config: Dict[str, Any]):
        self.transformation_config = transformation_config
    
    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single record"""
        transformed = record.copy()
        
        for field, config in self.transformation_config.items():
            if field in transformed:
                transform_type = config.get('type')
                
                if transform_type == 'uppercase':
                    transformed[field] = str(transformed[field]).upper()
                elif transform_type == 'multiply':
                    transformed[field] = transformed[field] * config['factor']
                elif transform_type == 'date_format':
                    # Convert date format
                    transformed[field] = datetime.strptime(
                        transformed[field], config['input_format']
                    ).strftime(config['output_format'])
        
        return transformed

class DataLoader:
    """Responsible only for loading data to destinations"""
    
    def __init__(self, connection_config: Dict[str, str]):
        self.connection_config = connection_config
    
    def load_batch(self, records: List[Dict[str, Any]], target_table: str) -> bool:
        """Load batch of records to target table"""
        try:
            # Implementation would use actual database connector
            logging.info(f"Loading {len(records)} records to {target_table}")
            return True
        except Exception as e:
            logging.error(f"Failed to load data: {str(e)}")
            return False

# 2. Open/Closed Principle (OCP)
# Classes should be open for extension but closed for modification

class MetricCalculator(ABC):
    """Abstract base class for metric calculations"""
    
    @abstractmethod
    def calculate(self, data: List[Dict[str, Any]]) -> float:
        """Calculate metric from data"""
        pass
    
    @abstractmethod
    def get_metric_name(self) -> str:
        """Get name of the metric"""
        pass

class CustomerLTVCalculator(MetricCalculator):
    """Calculate Customer Lifetime Value"""
    
    def calculate(self, customer_data: List[Dict[str, Any]]) -> float:
        total_ltv = 0
        for customer in customer_data:
            avg_order_value = customer.get('total_revenue', 0) / max(customer.get('order_count', 1), 1)
            purchase_frequency = customer.get('order_count', 0) / 12  # Monthly frequency
            customer_lifespan = 24  # Assumed 2 years
            
            ltv = avg_order_value * purchase_frequency * customer_lifespan
            total_ltv += ltv
        
        return total_ltv / len(customer_data) if customer_data else 0
    
    def get_metric_name(self) -> str:
        return "average_customer_ltv"

class ChurnRateCalculator(MetricCalculator):
    """Calculate customer churn rate"""
    
    def calculate(self, customer_data: List[Dict[str, Any]]) -> float:
        total_customers = len(customer_data)
        churned_customers = sum(1 for customer in customer_data 
                              if customer.get('days_since_last_order', 0) > 365)
        
        return (churned_customers / total_customers) * 100 if total_customers > 0 else 0
    
    def get_metric_name(self) -> str:
        return "churn_rate_percentage"

# Easy to add new calculators without modifying existing code
class MonthlyRevenueCalculator(MetricCalculator):
    """Calculate monthly recurring revenue"""
    
    def calculate(self, subscription_data: List[Dict[str, Any]]) -> float:
        return sum(sub.get('monthly_amount', 0) for sub in subscription_data 
                  if sub.get('status') == 'active')
    
    def get_metric_name(self) -> str:
        return "monthly_recurring_revenue"

# 3. Liskov Substitution Principle (LSP)
# Objects of a superclass should be replaceable with objects of its subclasses

class DataSource(ABC):
    """Abstract data source interface"""
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to data source"""
        pass
    
    @abstractmethod
    def fetch_data(self, query: str) -> List[Dict[str, Any]]:
        """Fetch data using query"""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close connection"""
        pass

class DatabaseSource(DataSource):
    """Database-based data source"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.connection = None
    
    def connect(self) -> bool:
        # Simulate database connection
        self.connection = f"Connected to {self.connection_string}"
        return True
    
    def fetch_data(self, query: str) -> List[Dict[str, Any]]:
        if not self.connection:
            raise ValueError("Not connected to database")
        
        # Simulate query execution
        return [{'id': 1, 'name': 'Customer 1'}, {'id': 2, 'name': 'Customer 2'}]
    
    def close(self) -> None:
        self.connection = None

class APISource(DataSource):
    """API-based data source"""
    
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key
        self.session = None
    
    def connect(self) -> bool:
        import requests
        self.session = requests.Session()
        self.session.headers.update({'Authorization': f'Bearer {self.api_key}'})
        return True
    
    def fetch_data(self, endpoint: str) -> List[Dict[str, Any]]:
        if not self.session:
            raise ValueError("Not connected to API")
        
        # Simulate API call
        return [{'customer_id': 'CUST001', 'email': 'customer@example.com'}]
    
    def close(self) -> None:
        if self.session:
            self.session.close()
            self.session = None

# Both implementations can be used interchangeably
def process_data_from_source(source: DataSource, query_or_endpoint: str):
    """Process data from any data source implementation"""
    source.connect()
    try:
        data = source.fetch_data(query_or_endpoint)
        # Process data regardless of source type
        return len(data)
    finally:
        source.close()

# 4. Interface Segregation Principle (ISP)
# No client should be forced to depend on methods it doesn't use

class Readable(Protocol):
    """Interface for readable data sources"""
    def read_data(self) -> List[Dict[str, Any]]: ...

class Writable(Protocol):
    """Interface for writable data destinations"""
    def write_data(self, data: List[Dict[str, Any]]) -> bool: ...

class Transformable(Protocol):
    """Interface for data transformation"""
    def transform_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]: ...

class CSVFileHandler:
    """Handles CSV files - implements only needed interfaces"""
    
    def __init__(self, filename: str):
        self.filename = filename
    
    def read_data(self) -> List[Dict[str, Any]]:
        """Implement Readable interface"""
        import csv
        with open(self.filename, 'r') as f:
            return list(csv.DictReader(f))
    
    def write_data(self, data: List[Dict[str, Any]]) -> bool:
        """Implement Writable interface"""
        import csv
        with open(self.filename, 'w', newline='') as f:
            if data:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
        return True

class DataTransformationEngine:
    """Only depends on transformation interface"""
    
    def __init__(self, transformable: Transformable):
        self.transformable = transformable
    
    def process(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return self.transformable.transform_data(data)

# 5. Dependency Inversion Principle (DIP)
# Depend on abstractions, not concretions

@dataclass
class AnalyticsPipelineConfig:
    """Configuration for analytics pipeline"""
    source_config: Dict[str, Any]
    transformation_config: Dict[str, Any]
    destination_config: Dict[str, Any]
    validation_rules: Dict[str, Any] = field(default_factory=dict)

class AnalyticsPipeline:
    """High-level analytics pipeline that depends on abstractions"""
    
    def __init__(self, 
                 data_source: DataSource,
                 validator: DataValidator,
                 transformer: DataTransformer,
                 loader: DataLoader,
                 metric_calculators: List[MetricCalculator]):
        # Depend on abstractions, not concrete implementations
        self.data_source = data_source
        self.validator = validator
        self.transformer = transformer
        self.loader = loader
        self.metric_calculators = metric_calculators
    
    def execute(self, query: str, target_table: str) -> Dict[str, Any]:
        """Execute the complete analytics pipeline"""
        results = {
            'processed_records': 0,
            'validation_errors': 0,
            'metrics': {}
        }
        
        # Extract data
        self.data_source.connect()
        try:
            raw_data = self.data_source.fetch_data(query)
            
            valid_records = []
            validation_errors = 0
            
            # Validate and transform data
            for record in raw_data:
                is_valid, errors = self.validator.validate_record(record)
                
                if is_valid:
                    transformed_record = self.transformer.transform_record(record)
                    valid_records.append(transformed_record)
                else:
                    validation_errors += 1
                    logging.warning(f"Validation errors: {errors}")
            
            # Load valid data
            if valid_records:
                success = self.loader.load_batch(valid_records, target_table)
                if success:
                    results['processed_records'] = len(valid_records)
                
                # Calculate metrics
                for calculator in self.metric_calculators:
                    metric_value = calculator.calculate(valid_records)
                    results['metrics'][calculator.get_metric_name()] = metric_value
            
            results['validation_errors'] = validation_errors
            
        finally:
            self.data_source.close()
        
        return results

# Design Patterns for Analytics Engineering

# 1. Factory Pattern for creating data connectors
class DataSourceFactory:
    """Factory for creating different types of data sources"""
    
    @staticmethod
    def create_source(source_type: str, config: Dict[str, Any]) -> DataSource:
        """Create data source based on type"""
        if source_type == 'database':
            return DatabaseSource(config['connection_string'])
        elif source_type == 'api':
            return APISource(config['base_url'], config['api_key'])
        else:
            raise ValueError(f"Unknown source type: {source_type}")

# 2. Observer Pattern for pipeline monitoring
class PipelineEvent:
    """Pipeline event data"""
    def __init__(self, event_type: str, message: str, data: Dict[str, Any] = None):
        self.event_type = event_type
        self.message = message
        self.data = data or {}
        self.timestamp = datetime.now()

class PipelineObserver(ABC):
    """Abstract observer for pipeline events"""
    
    @abstractmethod
    def notify(self, event: PipelineEvent) -> None:
        """Handle pipeline event"""
        pass

class LoggingObserver(PipelineObserver):
    """Observer that logs pipeline events"""
    
    def notify(self, event: PipelineEvent) -> None:
        logging.info(f"[{event.timestamp}] {event.event_type}: {event.message}")

class MetricsObserver(PipelineObserver):
    """Observer that collects pipeline metrics"""
    
    def __init__(self):
        self.metrics = []
    
    def notify(self, event: PipelineEvent) -> None:
        if event.event_type == 'METRIC_CALCULATED':
            self.metrics.append({
                'timestamp': event.timestamp,
                'metric_name': event.data.get('metric_name'),
                'value': event.data.get('value')
            })

class ObservablePipeline(AnalyticsPipeline):
    """Pipeline with observer support"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.observers: List[PipelineObserver] = []
    
    def add_observer(self, observer: PipelineObserver) -> None:
        """Add pipeline observer"""
        self.observers.append(observer)
    
    def notify_observers(self, event: PipelineEvent) -> None:
        """Notify all observers of event"""
        for observer in self.observers:
            observer.notify(event)
    
    def execute(self, query: str, target_table: str) -> Dict[str, Any]:
        """Execute pipeline with event notifications"""
        self.notify_observers(PipelineEvent('PIPELINE_START', 'Starting pipeline execution'))
        
        try:
            results = super().execute(query, target_table)
            
            # Notify metrics
            for metric_name, value in results.get('metrics', {}).items():
                self.notify_observers(PipelineEvent(
                    'METRIC_CALCULATED',
                    f"Calculated {metric_name}",
                    {'metric_name': metric_name, 'value': value}
                ))
            
            self.notify_observers(PipelineEvent('PIPELINE_SUCCESS', 'Pipeline completed successfully'))
            return results
            
        except Exception as e:
            self.notify_observers(PipelineEvent('PIPELINE_ERROR', f'Pipeline failed: {str(e)}'))
            raise

# 3. Strategy Pattern for different processing strategies
class ProcessingStrategy(ABC):
    """Abstract processing strategy"""
    
    @abstractmethod
    def process_batch(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process batch of data"""
        pass

class FastProcessingStrategy(ProcessingStrategy):
    """Fast processing with minimal transformations"""
    
    def process_batch(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # Minimal processing for speed
        return [{'processed': True, **record} for record in data]

class ThoroughProcessingStrategy(ProcessingStrategy):
    """Thorough processing with comprehensive transformations"""
    
    def process_batch(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        processed = []
        for record in data:
            # Comprehensive processing
            processed_record = {
                'original_id': record.get('id'),
                'processed_at': datetime.now().isoformat(),
                'validation_score': self._calculate_quality_score(record),
                **record
            }
            processed.append(processed_record)
        return processed
    
    def _calculate_quality_score(self, record: Dict[str, Any]) -> float:
        # Calculate data quality score
        return 0.95  # Placeholder

class ContextualProcessor:
    """Processor that uses different strategies based on context"""
    
    def __init__(self, strategy: ProcessingStrategy):
        self.strategy = strategy
    
    def set_strategy(self, strategy: ProcessingStrategy):
        """Change processing strategy"""
        self.strategy = strategy
    
    def process(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process using current strategy"""
        return self.strategy.process_batch(data)

# Usage example combining all patterns
def create_production_pipeline(config: AnalyticsPipelineConfig) -> ObservablePipeline:
    """Factory method to create production-ready pipeline"""
    
    # Create components using factories and dependency injection
    data_source = DataSourceFactory.create_source(
        config.source_config['type'], 
        config.source_config
    )
    
    validator = DataValidator(config.validation_rules)
    transformer = DataTransformer(config.transformation_config)
    loader = DataLoader(config.destination_config)
    
    # Create metric calculators
    metric_calculators = [
        CustomerLTVCalculator(),
        ChurnRateCalculator(),
        MonthlyRevenueCalculator()
    ]
    
    # Create observable pipeline
    pipeline = ObservablePipeline(
        data_source=data_source,
        validator=validator,
        transformer=transformer,
        loader=loader,
        metric_calculators=metric_calculators
    )
    
    # Add observers
    pipeline.add_observer(LoggingObserver())
    pipeline.add_observer(MetricsObserver())
    
    return pipeline
```

**Key Design Benefits**:
- **Modularity**: Each component has a single responsibility
- **Extensibility**: Easy to add new metric calculators, data sources, etc.
- **Testability**: Components can be easily mocked and unit tested  
- **Maintainability**: Changes in one component don't affect others
- **Reusability**: Components can be reused across different pipelines

---

### API Integration and Data Processing (21-30)

**Q4: How do you build robust API integrations for data ingestion with proper error handling, rate limiting, and authentication?**

**Answer**:
**Comprehensive API Integration Framework**:

```python
import requests
import time
import json
import logging
from typing import Dict, List, Optional, Any, Generator
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import wraps
import asyncio
import aiohttp
import backoff
from enum import Enum
import hashlib
import pickle
import os

# 1. Configuration and Authentication Management
class AuthType(Enum):
    BEARER_TOKEN = "bearer"
    API_KEY = "api_key" 
    OAUTH2 = "oauth2"
    BASIC = "basic"

@dataclass
class APIConfig:
    """Comprehensive API configuration"""
    base_url: str
    auth_type: AuthType
    auth_credentials: Dict[str, str]
    rate_limit: int = 100  # requests per minute
    timeout: int = 30  # seconds
    retry_attempts: int = 3
    retry_backoff: float = 1.0  # seconds
    max_concurrent: int = 10
    cache_ttl: int = 300  # seconds

class APIAuthenticator:
    """Handles different authentication methods"""
    
    def __init__(self, auth_type: AuthType, credentials: Dict[str, str]):
        self.auth_type = auth_type
        self.credentials = credentials
        self.token_cache = {}
        
    def get_headers(self) -> Dict[str, str]:
        """Get authentication headers"""
        if self.auth_type == AuthType.BEARER_TOKEN:
            return {'Authorization': f"Bearer {self.credentials['token']}"}
        
        elif self.auth_type == AuthType.API_KEY:
            key_name = self.credentials.get('key_name', 'X-API-Key')
            return {key_name: self.credentials['api_key']}
        
        elif self.auth_type == AuthType.OAUTH2:
            token = self._get_oauth2_token()
            return {'Authorization': f"Bearer {token}"}
        
        return {}
    
    def _get_oauth2_token(self) -> str:
        """Get OAuth2 token with caching"""
        cache_key = 'oauth2_token'
        
        if cache_key in self.token_cache:
            token_data = self.token_cache[cache_key]
            if datetime.now() < token_data['expires_at']:
                return token_data['token']
        
        # Request new token
        token_url = self.credentials['token_url']
        client_id = self.credentials['client_id']
        client_secret = self.credentials['client_secret']
        
        response = requests.post(token_url, data={
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret
        })
        
        if response.status_code == 200:
            token_data = response.json()
            expires_in = token_data.get('expires_in', 3600)
            
            self.token_cache[cache_key] = {
                'token': token_data['access_token'],
                'expires_at': datetime.now() + timedelta(seconds=expires_in - 60)
            }
            
            return token_data['access_token']
        
        raise Exception(f"Failed to get OAuth2 token: {response.text}")

# 2. Rate Limiting Implementation
class RateLimiter:
    """Token bucket rate limiter"""
    
    def __init__(self, rate_limit: int, time_window: int = 60):
        self.rate_limit = rate_limit
        self.time_window = time_window
        self.tokens = rate_limit
        self.last_refill = time.time()
    
    def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens, return True if successful"""
        now = time.time()
        
        # Refill tokens based on elapsed time
        elapsed = now - self.last_refill
        tokens_to_add = elapsed * (self.rate_limit / self.time_window)
        self.tokens = min(self.rate_limit, self.tokens + tokens_to_add)
        self.last_refill = now
        
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        
        return False
    
    def wait_time(self, tokens: int = 1) -> float:
        """Calculate wait time for tokens to be available"""
        if self.tokens >= tokens:
            return 0
        
        tokens_needed = tokens - self.tokens
        return tokens_needed * (self.time_window / self.rate_limit)

# 3. Robust API Client with Comprehensive Error Handling
class APIResponse:
    """Structured API response"""
    
    def __init__(self, status_code: int, data: Any, headers: Dict[str, str], 
                 response_time: float, url: str):
        self.status_code = status_code
        self.data = data
        self.headers = headers
        self.response_time = response_time
        self.url = url
        self.timestamp = datetime.now()
    
    @property
    def is_success(self) -> bool:
        return 200 <= self.status_code < 300
    
    @property
    def is_rate_limited(self) -> bool:
        return self.status_code == 429
    
    @property
    def is_server_error(self) -> bool:
        return 500 <= self.status_code < 600

class APIException(Exception):
    """Custom API exception with rich context"""
    
    def __init__(self, message: str, status_code: int = None, 
                 response_data: Any = None, url: str = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data
        self.url = url
        self.timestamp = datetime.now()

class RobustAPIClient:
    """Production-ready API client with comprehensive error handling"""
    
    def __init__(self, config: APIConfig):
        self.config = config
        self.authenticator = APIAuthenticator(config.auth_type, config.auth_credentials)
        self.rate_limiter = RateLimiter(config.rate_limit)
        self.session = requests.Session()
        self.request_cache = {}
        
        # Configure session defaults
        self.session.timeout = config.timeout
        self.session.headers.update({
            'User-Agent': 'Analytics-Pipeline/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
    
    @backoff.on_exception(
        backoff.expo,
        (requests.RequestException, APIException),
        max_tries=3,
        max_time=300,
        giveup=lambda e: hasattr(e, 'status_code') and 400 <= e.status_code < 500
    )
    def _make_request(self, method: str, url: str, **kwargs) -> APIResponse:
        """Make HTTP request with retry logic"""
        
        # Rate limiting
        if not self.rate_limiter.acquire():
            wait_time = self.rate_limiter.wait_time()
            logging.info(f"Rate limit reached, waiting {wait_time:.2f} seconds")
            time.sleep(wait_time)
            self.rate_limiter.acquire()
        
        # Add authentication headers
        headers = kwargs.get('headers', {})
        headers.update(self.authenticator.get_headers())
        kwargs['headers'] = headers
        
        # Make request
        start_time = time.time()
        full_url = f"{self.config.base_url.rstrip('/')}/{url.lstrip('/')}"
        
        try:
            response = self.session.request(method, full_url, **kwargs)
            response_time = time.time() - start_time
            
            # Parse response data
            try:
                data = response.json() if response.content else None
            except json.JSONDecodeError:
                data = response.text
            
            api_response = APIResponse(
                status_code=response.status_code,
                data=data,
                headers=dict(response.headers),
                response_time=response_time,
                url=full_url
            )
            
            # Handle different response types
            if api_response.is_rate_limited:
                retry_after = int(response.headers.get('Retry-After', 60))
                logging.warning(f"Rate limited, retrying after {retry_after} seconds")
                time.sleep(retry_after)
                raise APIException(f"Rate limited", status_code=429, url=full_url)
            
            elif api_response.is_server_error:
                raise APIException(
                    f"Server error: {response.status_code}",
                    status_code=response.status_code,
                    response_data=data,
                    url=full_url
                )
            
            elif not api_response.is_success:
                raise APIException(
                    f"API error: {response.status_code} - {data}",
                    status_code=response.status_code,
                    response_data=data,
                    url=full_url
                )
            
            # Log successful request
            logging.debug(f"API call successful: {method} {full_url} ({response_time:.2f}s)")
            
            return api_response
            
        except requests.RequestException as e:
            logging.error(f"Request failed: {method} {full_url} - {str(e)}")
            raise APIException(f"Request failed: {str(e)}", url=full_url)
    
    def get(self, endpoint: str, params: Dict[str, Any] = None, 
            use_cache: bool = True) -> APIResponse:
        """GET request with caching support"""
        
        # Check cache if enabled
        if use_cache and self.config.cache_ttl > 0:
            cache_key = self._get_cache_key('GET', endpoint, params)
            cached_response = self._get_cached_response(cache_key)
            if cached_response:
                return cached_response
        
        response = self._make_request('GET', endpoint, params=params)
        
        # Cache successful responses
        if use_cache and response.is_success and self.config.cache_ttl > 0:
            cache_key = self._get_cache_key('GET', endpoint, params)
            self._cache_response(cache_key, response)
        
        return response
    
    def post(self, endpoint: str, data: Dict[str, Any] = None, 
             json_data: Dict[str, Any] = None) -> APIResponse:
        """POST request"""
        kwargs = {}
        if data:
            kwargs['data'] = data
        if json_data:
            kwargs['json'] = json_data
        
        return self._make_request('POST', endpoint, **kwargs)
    
    def paginated_get(self, endpoint: str, params: Dict[str, Any] = None,
                     page_param: str = 'page', limit_param: str = 'limit',
                     max_pages: int = None) -> Generator[APIResponse, None, None]:
        """Handle paginated API responses"""
        
        page = 1
        params = params or {}
        params[limit_param] = params.get(limit_param, 100)
        
        while True:
            params[page_param] = page
            response = self.get(endpoint, params, use_cache=False)
            
            yield response
            
            # Check if there are more pages
            if not self._has_more_pages(response, page):
                break
                
            if max_pages and page >= max_pages:
                break
                
            page += 1
    
    def _has_more_pages(self, response: APIResponse, current_page: int) -> bool:
        """Determine if there are more pages (customize based on API)"""
        
        if not response.is_success:
            return False
        
        data = response.data
        
        # Common pagination patterns
        if isinstance(data, dict):
            # Pattern 1: has_more field
            if 'has_more' in data:
                return data['has_more']
            
            # Pattern 2: pagination metadata
            if 'pagination' in data:
                pagination = data['pagination']
                return current_page < pagination.get('total_pages', current_page)
            
            # Pattern 3: data array length
            if 'data' in data:
                data_array = data['data']
                return len(data_array) > 0  # Assume empty array means no more pages
        
        return False
    
    def _get_cache_key(self, method: str, endpoint: str, params: Dict[str, Any] = None) -> str:
        """Generate cache key for request"""
        key_data = f"{method}:{endpoint}:{json.dumps(params or {}, sort_keys=True)}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _get_cached_response(self, cache_key: str) -> Optional[APIResponse]:
        """Get cached response if still valid"""
        cache_file = f".api_cache/{cache_key}.cache"
        
        if os.path.exists(cache_file):
            file_age = time.time() - os.path.getmtime(cache_file)
            if file_age < self.config.cache_ttl:
                try:
                    with open(cache_file, 'rb') as f:
                        return pickle.load(f)
                except:
                    os.remove(cache_file)
        
        return None
    
    def _cache_response(self, cache_key: str, response: APIResponse):
        """Cache API response"""
        os.makedirs('.api_cache', exist_ok=True)
        cache_file = f".api_cache/{cache_key}.cache"
        
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(response, f)
        except Exception as e:
            logging.warning(f"Failed to cache response: {str(e)}")

# 4. Async API Client for High-Performance Concurrent Processing
class AsyncAPIClient:
    """Async API client for concurrent processing"""
    
    def __init__(self, config: APIConfig):
        self.config = config
        self.authenticator = APIAuthenticator(config.auth_type, config.auth_credentials)
        self.semaphore = asyncio.Semaphore(config.max_concurrent)
        self.rate_limiter = RateLimiter(config.rate_limit)
    
    async def _make_async_request(self, session: aiohttp.ClientSession, 
                                 method: str, url: str, **kwargs) -> APIResponse:
        """Make async HTTP request"""
        
        async with self.semaphore:  # Limit concurrent requests
            
            # Rate limiting
            while not self.rate_limiter.acquire():
                wait_time = self.rate_limiter.wait_time()
                await asyncio.sleep(wait_time)
            
            # Add authentication
            headers = kwargs.get('headers', {})
            headers.update(self.authenticator.get_headers())
            kwargs['headers'] = headers
            
            start_time = time.time()
            full_url = f"{self.config.base_url.rstrip('/')}/{url.lstrip('/')}"
            
            try:
                async with session.request(method, full_url, **kwargs) as response:
                    response_time = time.time() - start_time
                    
                    try:
                        data = await response.json()
                    except:
                        data = await response.text()
                    
                    return APIResponse(
                        status_code=response.status,
                        data=data,
                        headers=dict(response.headers),
                        response_time=response_time,
                        url=full_url
                    )
            
            except aiohttp.ClientError as e:
                raise APIException(f"Async request failed: {str(e)}", url=full_url)
    
    async def batch_get(self, endpoints: List[str], 
                       params_list: List[Dict[str, Any]] = None) -> List[APIResponse]:
        """Execute multiple GET requests concurrently"""
        
        if params_list is None:
            params_list = [{}] * len(endpoints)
        
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = []
            
            for endpoint, params in zip(endpoints, params_list):
                task = self._make_async_request(session, 'GET', endpoint, params=params)
                tasks.append(task)
            
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Handle exceptions
            results = []
            for i, response in enumerate(responses):
                if isinstance(response, Exception):
                    logging.error(f"Request {i} failed: {str(response)}")
                    # Create error response
                    error_response = APIResponse(
                        status_code=500,
                        data={'error': str(response)},
                        headers={},
                        response_time=0,
                        url=endpoints[i]
                    )
                    results.append(error_response)
                else:
                    results.append(response)
            
            return results

# 5. Data Processing Integration
class APIDataProcessor:
    """Process data from API endpoints with comprehensive error handling"""
    
    def __init__(self, api_client: RobustAPIClient):
        self.api_client = api_client
        self.processing_stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'total_records': 0,
            'processing_errors': 0
        }
    
    def extract_customer_data(self, customer_ids: List[str]) -> List[Dict[str, Any]]:
        """Extract customer data with batch processing"""
        
        all_customers = []
        batch_size = 50  # Process in batches to avoid overwhelming API
        
        for i in range(0, len(customer_ids), batch_size):
            batch = customer_ids[i:i + batch_size]
            
            try:
                # Request batch of customers
                params = {'customer_ids': ','.join(batch)}
                response = self.api_client.get('customers', params)
                
                self.processing_stats['total_requests'] += 1
                
                if response.is_success:
                    self.processing_stats['successful_requests'] += 1
                    
                    customers = response.data.get('customers', [])
                    processed_customers = []
                    
                    for customer in customers:
                        try:
                            processed_customer = self._process_customer_record(customer)
                            processed_customers.append(processed_customer)
                            self.processing_stats['total_records'] += 1
                        
                        except Exception as e:
                            logging.error(f"Error processing customer {customer.get('id', 'unknown')}: {str(e)}")
                            self.processing_stats['processing_errors'] += 1
                    
                    all_customers.extend(processed_customers)
                
                else:
                    self.processing_stats['failed_requests'] += 1
                    logging.error(f"API request failed: {response.status_code}")
            
            except Exception as e:
                self.processing_stats['failed_requests'] += 1
                logging.error(f"Batch processing failed: {str(e)}")
            
            # Rate limiting between batches
            time.sleep(0.1)
        
        return all_customers
    
    def _process_customer_record(self, customer_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process and standardize customer record"""
        
        processed = {
            'customer_id': customer_data.get('id'),
            'email': customer_data.get('email', '').lower().strip(),
            'name': customer_data.get('name', '').strip(),
            'created_at': customer_data.get('created_at'),
            'updated_at': customer_data.get('updated_at'),
            'total_orders': customer_data.get('total_orders', 0),
            'total_spent': float(customer_data.get('total_spent', 0)),
            'status': customer_data.get('status', 'active'),
            'extracted_at': datetime.now().isoformat()
        }
        
        # Data quality validation
        if not processed['customer_id']:
            raise ValueError("Missing customer ID")
        
        if not processed['email'] or '@' not in processed['email']:
            logging.warning(f"Invalid email for customer {processed['customer_id']}")
            processed['email_valid'] = False
        else:
            processed['email_valid'] = True
        
        return processed
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Get summary of processing statistics"""
        
        total_requests = self.processing_stats['total_requests']
        success_rate = (self.processing_stats['successful_requests'] / total_requests * 100) if total_requests > 0 else 0
        
        return {
            **self.processing_stats,
            'success_rate_percent': round(success_rate, 2),
            'avg_records_per_request': (
                self.processing_stats['total_records'] / 
                max(self.processing_stats['successful_requests'], 1)
            )
        }

# 6. Usage Example with Complete Error Handling
def create_production_api_integration():
    """Example of production-ready API integration setup"""
    
    # Configuration
    config = APIConfig(
        base_url="https://api.example.com/v1",
        auth_type=AuthType.OAUTH2,
        auth_credentials={
            'token_url': 'https://api.example.com/oauth/token',
            'client_id': os.getenv('API_CLIENT_ID'),
            'client_secret': os.getenv('API_CLIENT_SECRET')
        },
        rate_limit=300,  # 300 requests per minute
        timeout=30,
        retry_attempts=3,
        cache_ttl=600  # 10 minutes
    )
    
    # Create clients
    sync_client = RobustAPIClient(config)
    async_client = AsyncAPIClient(config)
    processor = APIDataProcessor(sync_client)
    
    return sync_client, async_client, processor

# Example usage
if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Create API integration
    sync_client, async_client, processor = create_production_api_integration()
    
    # Extract customer data
    customer_ids = ['CUST001', 'CUST002', 'CUST003']
    customers = processor.extract_customer_data(customer_ids)
    
    # Get processing summary
    summary = processor.get_processing_summary()
    print(f"Processing complete: {summary}")
```

**Key Integration Features**:
- **Comprehensive Authentication**: Support for multiple auth types (Bearer, API Key, OAuth2)
- **Intelligent Rate Limiting**: Token bucket algorithm prevents API quota exhaustion
- **Robust Error Handling**: Automatic retries with exponential backoff for transient failures  
- **Response Caching**: Reduces API calls and improves performance
- **Async Support**: High-performance concurrent processing for batch operations
- **Monitoring & Logging**: Detailed metrics and logging for production troubleshooting
- **Data Validation**: Built-in data quality checks and standardization
- **Production Ready**: Handles edge cases, network issues, and API changes gracefully

---

### Advanced Python Concepts (31-40)

**Q5: Implement advanced Python patterns for Analytics Engineering including decorators, context managers, metaclasses, and async programming.**

**Answer**:
**Advanced Python Patterns for Analytics Engineering**:

```python
import functools
import time
import logging
import asyncio
import aiofiles
from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
from contextlib import contextmanager, asynccontextmanager
from datetime import datetime, timedelta
import threading
import queue
import inspect
from dataclasses import dataclass
import json
import pickle
from pathlib import Path

# 1. Advanced Decorators for Analytics Engineering

def performance_monitor(include_args: bool = False, log_level: int = logging.INFO):
    """Decorator to monitor function performance with detailed metrics"""
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            start_memory = get_memory_usage()
            
            try:
                result = func(*args, **kwargs)
                
                end_time = time.time()
                end_memory = get_memory_usage()
                
                execution_time = end_time - start_time
                memory_delta = end_memory - start_memory
                
                # Log performance metrics
                log_message = f"Function {func.__name__} executed in {execution_time:.4f}s, memory delta: {memory_delta:.2f}MB"
                
                if include_args:
                    args_str = ", ".join([str(arg)[:50] for arg in args])
                    kwargs_str = ", ".join([f"{k}={str(v)[:50]}" for k, v in kwargs.items()])
                    log_message += f" with args: ({args_str}) kwargs: {{{kwargs_str}}}"
                
                logging.log(log_level, log_message)
                
                # Store metrics in result metadata if possible
                if hasattr(result, '__dict__'):
                    result.__dict__['_performance_metrics'] = {
                        'execution_time': execution_time,
                        'memory_delta': memory_delta,
                        'timestamp': datetime.now().isoformat()
                    }
                
                return result
                
            except Exception as e:
                end_time = time.time()
                execution_time = end_time - start_time
                
                logging.error(f"Function {func.__name__} failed after {execution_time:.4f}s with error: {str(e)}")
                raise
        
        return wrapper
    return decorator

def cache_with_ttl(ttl_seconds: int = 300, max_size: int = 128, 
                  key_func: Optional[Callable] = None):
    """Advanced caching decorator with TTL and custom key generation"""
    
    def decorator(func: Callable) -> Callable:
        cache = {}
        cache_times = {}
        
        def default_key_func(*args, **kwargs):
            return str(args) + str(sorted(kwargs.items()))
        
        key_generator = key_func or default_key_func
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key
            cache_key = key_generator(*args, **kwargs)
            
            # Check cache validity
            now = time.time()
            if cache_key in cache:
                if now - cache_times[cache_key] < ttl_seconds:
                    logging.debug(f"Cache hit for {func.__name__} with key {cache_key[:50]}")
                    return cache[cache_key]
                else:
                    # Remove expired entry
                    del cache[cache_key]
                    del cache_times[cache_key]
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            
            # Manage cache size
            if len(cache) >= max_size:
                # Remove oldest entry
                oldest_key = min(cache_times.keys(), key=cache_times.get)
                del cache[oldest_key]
                del cache_times[oldest_key]
            
            cache[cache_key] = result
            cache_times[cache_key] = now
            
            logging.debug(f"Cached result for {func.__name__} with key {cache_key[:50]}")
            return result
        
        # Add cache management methods
        wrapper.cache_info = lambda: {
            'size': len(cache),
            'max_size': max_size,
            'ttl_seconds': ttl_seconds,
            'oldest_entry': min(cache_times.values()) if cache_times else None
        }
        
        wrapper.cache_clear = lambda: (cache.clear(), cache_times.clear())
        
        return wrapper
    return decorator

def validate_types(**type_annotations):
    """Decorator for runtime type validation"""
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get function signature
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            
            # Validate types
            for param_name, expected_type in type_annotations.items():
                if param_name in bound_args.arguments:
                    value = bound_args.arguments[param_name]
                    if not isinstance(value, expected_type):
                        raise TypeError(
                            f"Parameter '{param_name}' must be of type {expected_type.__name__}, "
                            f"got {type(value).__name__}"
                        )
            
            return func(*args, **kwargs)
        return wrapper
    return decorator

# Usage examples for decorators
@performance_monitor(include_args=True)
@cache_with_ttl(ttl_seconds=600, max_size=64)
@validate_types(customer_ids=list, batch_size=int)
def process_customer_batch(customer_ids: List[str], batch_size: int = 100) -> Dict[str, Any]:
    """Process customer batch with monitoring, caching, and validation"""
    time.sleep(0.1)  # Simulate processing
    return {
        'processed_count': len(customer_ids),
        'batch_size': batch_size,
        'processing_time': datetime.now().isoformat()
    }

# 2. Advanced Context Managers for Resource Management

class DatabaseTransaction:
    """Context manager for database transactions with rollback support"""
    
    def __init__(self, connection, auto_commit: bool = True, 
                 isolation_level: str = 'READ_COMMITTED'):
        self.connection = connection
        self.auto_commit = auto_commit
        self.isolation_level = isolation_level
        self.transaction_started = False
        self.savepoints = []
    
    def __enter__(self):
        self.connection.execute(f"SET TRANSACTION ISOLATION LEVEL {self.isolation_level}")
        self.connection.execute("BEGIN")
        self.transaction_started = True
        logging.info(f"Transaction started with isolation level {self.isolation_level}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                if self.auto_commit:
                    self.connection.execute("COMMIT")
                    logging.info("Transaction committed successfully")
                else:
                    logging.info("Transaction prepared for manual commit")
            else:
                self.connection.execute("ROLLBACK")
                logging.error(f"Transaction rolled back due to error: {exc_val}")
        finally:
            self.transaction_started = False
            self.savepoints.clear()
    
    def create_savepoint(self, name: str):
        """Create a savepoint within the transaction"""
        if self.transaction_started:
            self.connection.execute(f"SAVEPOINT {name}")
            self.savepoints.append(name)
            logging.debug(f"Savepoint '{name}' created")
    
    def rollback_to_savepoint(self, name: str):
        """Rollback to a specific savepoint"""
        if name in self.savepoints:
            self.connection.execute(f"ROLLBACK TO SAVEPOINT {name}")
            # Remove savepoints created after this one
            idx = self.savepoints.index(name)
            self.savepoints = self.savepoints[:idx + 1]
            logging.info(f"Rolled back to savepoint '{name}'")

@contextmanager
def temporary_directory(cleanup: bool = True, prefix: str = "analytics_"):
    """Context manager for temporary directory with optional cleanup"""
    import tempfile
    import shutil
    
    temp_dir = tempfile.mkdtemp(prefix=prefix)
    logging.info(f"Created temporary directory: {temp_dir}")
    
    try:
        yield Path(temp_dir)
    finally:
        if cleanup:
            shutil.rmtree(temp_dir, ignore_errors=True)
            logging.info(f"Cleaned up temporary directory: {temp_dir}")

@contextmanager
def performance_timer(operation_name: str, log_slow_threshold: float = 1.0):
    """Context manager for timing operations with slow operation logging"""
    start_time = time.time()
    logging.info(f"Starting operation: {operation_name}")
    
    try:
        yield
    finally:
        elapsed_time = time.time() - start_time
        
        if elapsed_time > log_slow_threshold:
            logging.warning(f"Slow operation detected: {operation_name} took {elapsed_time:.2f}s")
        else:
            logging.info(f"Completed operation: {operation_name} in {elapsed_time:.2f}s")

# 3. Metaclasses for Analytics Engineering

class SingletonMeta(type):
    """Metaclass for implementing singleton pattern (useful for connection pools, config managers)"""
    
    _instances = {}
    _lock = threading.Lock()
    
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            with cls._lock:
                if cls not in cls._instances:
                    instance = super().__call__(*args, **kwargs)
                    cls._instances[cls] = instance
        return cls._instances[cls]

class ConfigManagerMeta(type):
    """Metaclass that automatically loads configuration on class creation"""
    
    def __new__(mcs, name, bases, namespace, **kwargs):
        # Add configuration loading to class
        config_file = kwargs.get('config_file', f"{name.lower()}_config.json")
        
        def load_config(cls):
            try:
                with open(config_file, 'r') as f:
                    cls._config = json.load(f)
                logging.info(f"Loaded configuration for {name} from {config_file}")
            except FileNotFoundError:
                cls._config = {}
                logging.warning(f"Configuration file {config_file} not found, using empty config")
        
        namespace['load_config'] = classmethod(load_config)
        namespace['_config_file'] = config_file
        
        cls = super().__new__(mcs, name, bases, namespace)
        cls.load_config()
        
        return cls

class ValidatedAttributeMeta(type):
    """Metaclass that adds automatic attribute validation to classes"""
    
    def __new__(mcs, name, bases, namespace):
        # Find attributes with type annotations
        annotations = namespace.get('__annotations__', {})
        
        for attr_name, attr_type in annotations.items():
            if not attr_name.startswith('_'):
                # Create property with validation
                private_name = f'_{attr_name}'
                
                def make_property(attr_name, attr_type, private_name):
                    def getter(self):
                        return getattr(self, private_name, None)
                    
                    def setter(self, value):
                        if value is not None and not isinstance(value, attr_type):
                            raise TypeError(f"{attr_name} must be of type {attr_type.__name__}")
                        setattr(self, private_name, value)
                    
                    return property(getter, setter)
                
                namespace[attr_name] = make_property(attr_name, attr_type, private_name)
        
        return super().__new__(mcs, name, bases, namespace)

# Example usage of metaclasses
class ConnectionPool(metaclass=SingletonMeta):
    """Singleton connection pool"""
    
    def __init__(self, max_connections: int = 10):
        if hasattr(self, '_initialized'):
            return
        
        self.max_connections = max_connections
        self.active_connections = 0
        self._initialized = True
        logging.info(f"Initialized connection pool with max {max_connections} connections")

class AnalyticsConfig(metaclass=ConfigManagerMeta, config_file='analytics_config.json'):
    """Configuration manager with automatic loading"""
    
    @classmethod
    def get(cls, key: str, default=None):
        return cls._config.get(key, default)

class Customer(metaclass=ValidatedAttributeMeta):
    """Customer class with automatic attribute validation"""
    
    customer_id: str
    email: str
    name: str
    ltv: float
    order_count: int
    
    def __init__(self, customer_id: str, email: str, name: str, ltv: float, order_count: int):
        self.customer_id = customer_id
        self.email = email
        self.name = name
        self.ltv = ltv
        self.order_count = order_count

# 4. Advanced Async Programming for Analytics

class AsyncDataProcessor:
    """Advanced async processor with concurrency control and error handling"""
    
    def __init__(self, max_concurrent: int = 10, timeout: float = 30.0):
        self.max_concurrent = max_concurrent
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.results_queue = asyncio.Queue()
        self.error_queue = asyncio.Queue()
    
    async def process_batch_async(self, items: List[Any], 
                                processor_func: Callable) -> List[Any]:
        """Process items asynchronously with concurrency control"""
        
        async def process_item_with_semaphore(item):
            async with self.semaphore:
                try:
                    result = await asyncio.wait_for(
                        processor_func(item), 
                        timeout=self.timeout
                    )
                    await self.results_queue.put(result)
                    return result
                except asyncio.TimeoutError:
                    error_msg = f"Processing timeout for item {item}"
                    await self.error_queue.put(error_msg)
                    logging.error(error_msg)
                    return None
                except Exception as e:
                    error_msg = f"Processing error for item {item}: {str(e)}"
                    await self.error_queue.put(error_msg)
                    logging.error(error_msg)
                    return None
        
        # Create tasks for all items
        tasks = [process_item_with_semaphore(item) for item in items]
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out None results and exceptions
        successful_results = [r for r in results if r is not None and not isinstance(r, Exception)]
        
        return successful_results
    
    async def stream_process(self, data_stream: AsyncIterator, 
                           processor_func: Callable) -> AsyncIterator[Any]:
        """Stream processing with async generator"""
        
        async for item in data_stream:
            async with self.semaphore:
                try:
                    result = await processor_func(item)
                    yield result
                except Exception as e:
                    logging.error(f"Stream processing error: {str(e)}")

@asynccontextmanager
async def async_database_connection(connection_string: str):
    """Async context manager for database connections"""
    connection = None
    try:
        # Simulate async connection establishment
        await asyncio.sleep(0.1)
        connection = f"Connected to {connection_string}"
        logging.info(f"Established async database connection")
        yield connection
    finally:
        if connection:
            # Simulate async connection cleanup
            await asyncio.sleep(0.05)
            logging.info("Closed async database connection")

async def async_file_processor(file_paths: List[str]) -> List[Dict[str, Any]]:
    """Async file processing with aiofiles"""
    
    async def process_file(file_path: str) -> Dict[str, Any]:
        async with aiofiles.open(file_path, 'r') as f:
            content = await f.read()
            return {
                'file_path': file_path,
                'line_count': len(content.split('\n')),
                'char_count': len(content),
                'processed_at': datetime.now().isoformat()
            }
    
    # Process files concurrently
    processor = AsyncDataProcessor(max_concurrent=5)
    results = await processor.process_batch_async(file_paths, process_file)
    
    return results

# 5. Advanced Generator and Iterator Patterns

class DataPipeline:
    """Advanced data pipeline using generator composition"""
    
    def __init__(self):
        self.transformations = []
    
    def add_transformation(self, transform_func: Callable):
        """Add transformation function to pipeline"""
        self.transformations.append(transform_func)
        return self  # Allow method chaining
    
    def process(self, data_source):
        """Process data through pipeline transformations"""
        
        def pipeline_generator():
            current_data = data_source
            
            # Apply transformations in sequence
            for transform in self.transformations:
                current_data = transform(current_data)
            
            # Yield processed items
            for item in current_data:
                yield item
        
        return pipeline_generator()

def batch_generator(data_stream, batch_size: int):
    """Convert stream into batches"""
    batch = []
    for item in data_stream:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    
    if batch:  # Yield remaining items
        yield batch

def filter_transform(data_stream, predicate: Callable):
    """Filter transformation for pipeline"""
    for item in data_stream:
        if predicate(item):
            yield item

def map_transform(data_stream, mapper: Callable):
    """Map transformation for pipeline"""
    for item in data_stream:
        yield mapper(item)

# Example usage of advanced patterns
async def analytics_processing_example():
    """Comprehensive example using all advanced patterns"""
    
    # Use context managers for resource management
    with temporary_directory() as temp_dir:
        with performance_timer("Complete analytics processing"):
            
            # Create test data files
            test_files = []
            for i in range(3):
                test_file = temp_dir / f"data_{i}.txt"
                with open(test_file, 'w') as f:
                    f.write(f"Test data file {i}\nWith multiple lines\nFor processing")
                test_files.append(str(test_file))
            
            # Async file processing
            file_results = await async_file_processor(test_files)
            logging.info(f"Processed {len(file_results)} files asynchronously")
            
            # Use metaclass-based singleton
            pool = ConnectionPool(max_connections=5)
            pool2 = ConnectionPool(max_connections=10)  # Same instance
            assert pool is pool2
            
            # Use decorated function with caching
            customer_ids = ['CUST001', 'CUST002', 'CUST003']
            result1 = process_customer_batch(customer_ids, batch_size=2)
            result2 = process_customer_batch(customer_ids, batch_size=2)  # Cached
            
            # Use data pipeline
            data_source = [
                {'id': 1, 'value': 10, 'active': True},
                {'id': 2, 'value': 5, 'active': False},
                {'id': 3, 'value': 15, 'active': True}
            ]
            
            pipeline = (DataPipeline()
                       .add_transformation(lambda data: filter_transform(data, lambda x: x['active']))
                       .add_transformation(lambda data: map_transform(data, lambda x: {**x, 'processed': True})))
            
            processed_data = list(pipeline.process(data_source))
            logging.info(f"Pipeline processed {len(processed_data)} items")
            
            return {
                'file_results': file_results,
                'cached_result': result1,
                'pipeline_result': processed_data
            }

def get_memory_usage() -> float:
    """Get current memory usage in MB"""
    import psutil
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024

# Run the example
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Run async example
    result = asyncio.run(analytics_processing_example())
    print(f"Processing complete: {json.dumps(result, indent=2, default=str)}")
```

**Advanced Pattern Benefits**:
- **Decorators**: Automatic performance monitoring, caching, and validation
- **Context Managers**: Guaranteed resource cleanup and transaction safety
- **Metaclasses**: Automatic configuration loading and attribute validation
- **Async Programming**: High-performance concurrent processing for I/O operations
- **Generators**: Memory-efficient data pipeline processing
- **Type Safety**: Runtime type validation for production reliability

These patterns enable robust, high-performance Analytics Engineering solutions that scale efficiently and handle errors gracefully.

---

### Data Engineering and ETL (41-50)

**Q6: Design comprehensive data validation, transformation, and loading systems using Python for Analytics Engineering pipelines.**

**Answer**:
**Comprehensive Data Pipeline System**:

```python
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Callable, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import json
import logging
import hashlib
from pathlib import Path
import sqlalchemy as sa
from sqlalchemy import create_engine, MetaData, Table
import great_expectations as ge
from great_expectations.core import ExpectationSuite
from pydantic import BaseModel, validator, Field
import pandera as pa
from pandera import Check, Column, DataFrameSchema
import dask.dataframe as dd

# 1. Advanced Data Validation Framework
class ValidationSeverity(Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"

class ValidationResult(BaseModel):
    """Structured validation result"""
    rule_name: str
    severity: ValidationSeverity
    passed: bool
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)
    affected_rows: int = 0
    timestamp: datetime = Field(default_factory=datetime.now)

@dataclass
class ValidationRule:
    """Data validation rule configuration"""
    name: str
    description: str
    validation_func: Callable[[pd.DataFrame], ValidationResult]
    severity: ValidationSeverity = ValidationSeverity.ERROR
    enabled: bool = True
    tags: List[str] = field(default_factory=list)

class AdvancedDataValidator:
    """Comprehensive data validation system"""
    
    def __init__(self):
        self.rules: List[ValidationRule] = []
        self.validation_history: List[ValidationResult] = []
    
    def add_rule(self, rule: ValidationRule):
        """Add validation rule to the validator"""
        self.rules.append(rule)
        logging.info(f"Added validation rule: {rule.name}")
    
    def validate_dataframe(self, df: pd.DataFrame, tags: Optional[List[str]] = None) -> List[ValidationResult]:
        """Validate DataFrame against all applicable rules"""
        results = []
        
        # Filter rules by tags if specified
        applicable_rules = self.rules
        if tags:
            applicable_rules = [rule for rule in self.rules if any(tag in rule.tags for tag in tags)]
        
        for rule in applicable_rules:
            if not rule.enabled:
                continue
                
            try:
                logging.debug(f"Executing validation rule: {rule.name}")
                result = rule.validation_func(df)
                results.append(result)
                
                # Log based on severity
                if result.severity == ValidationSeverity.ERROR and not result.passed:
                    logging.error(f"Validation failed: {result.message}")
                elif result.severity == ValidationSeverity.WARNING and not result.passed:
                    logging.warning(f"Validation warning: {result.message}")
                
            except Exception as e:
                error_result = ValidationResult(
                    rule_name=rule.name,
                    severity=ValidationSeverity.ERROR,
                    passed=False,
                    message=f"Validation rule execution failed: {str(e)}",
                    details={'exception': str(e)}
                )
                results.append(error_result)
                logging.error(f"Validation rule {rule.name} failed to execute: {str(e)}")
        
        # Store validation history
        self.validation_history.extend(results)
        
        return results
    
    def get_validation_summary(self, results: List[ValidationResult]) -> Dict[str, Any]:
        """Generate validation summary statistics"""
        total_rules = len(results)
        passed_rules = sum(1 for r in results if r.passed)
        failed_rules = total_rules - passed_rules
        
        errors = [r for r in results if r.severity == ValidationSeverity.ERROR and not r.passed]
        warnings = [r for r in results if r.severity == ValidationSeverity.WARNING and not r.passed]
        
        return {
            'total_rules_executed': total_rules,
            'rules_passed': passed_rules,
            'rules_failed': failed_rules,
            'error_count': len(errors),
            'warning_count': len(warnings),
            'pass_rate': (passed_rules / total_rules) * 100 if total_rules > 0 else 0,
            'errors': [{'rule': e.rule_name, 'message': e.message} for e in errors],
            'warnings': [{'rule': w.rule_name, 'message': w.message} for w in warnings]
        }

# Common validation rules
def create_completeness_rule(column: str, min_completeness: float = 0.95) -> ValidationRule:
    """Create completeness validation rule"""
    
    def validate_completeness(df: pd.DataFrame) -> ValidationResult:
        if column not in df.columns:
            return ValidationResult(
                rule_name=f"completeness_{column}",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message=f"Column '{column}' not found in DataFrame"
            )
        
        non_null_count = df[column].count()
        total_count = len(df)
        completeness_ratio = non_null_count / total_count if total_count > 0 else 0
        
        passed = completeness_ratio >= min_completeness
        affected_rows = total_count - non_null_count
        
        return ValidationResult(
            rule_name=f"completeness_{column}",
            severity=ValidationSeverity.ERROR,
            passed=passed,
            message=f"Column '{column}' completeness: {completeness_ratio:.2%} (required: {min_completeness:.2%})",
            details={
                'completeness_ratio': completeness_ratio,
                'non_null_count': non_null_count,
                'total_count': total_count
            },
            affected_rows=affected_rows
        )
    
    return ValidationRule(
        name=f"completeness_{column}",
        description=f"Validates that column '{column}' has at least {min_completeness:.1%} non-null values",
        validation_func=validate_completeness,
        tags=['completeness', 'data_quality']
    )

def create_uniqueness_rule(columns: Union[str, List[str]]) -> ValidationRule:
    """Create uniqueness validation rule"""
    
    cols = [columns] if isinstance(columns, str) else columns
    rule_name = f"uniqueness_{'_'.join(cols)}"
    
    def validate_uniqueness(df: pd.DataFrame) -> ValidationResult:
        missing_cols = [col for col in cols if col not in df.columns]
        if missing_cols:
            return ValidationResult(
                rule_name=rule_name,
                severity=ValidationSeverity.ERROR,
                passed=False,
                message=f"Columns not found: {missing_cols}"
            )
        
        total_count = len(df)
        unique_count = df[cols].drop_duplicates().shape[0]
        duplicate_count = total_count - unique_count
        
        passed = duplicate_count == 0
        
        return ValidationResult(
            rule_name=rule_name,
            severity=ValidationSeverity.ERROR,
            passed=passed,
            message=f"Found {duplicate_count} duplicate rows in columns {cols}",
            details={
                'total_rows': total_count,
                'unique_rows': unique_count,
                'duplicate_rows': duplicate_count
            },
            affected_rows=duplicate_count
        )
    
    return ValidationRule(
        name=rule_name,
        description=f"Validates uniqueness of columns {cols}",
        validation_func=validate_uniqueness,
        tags=['uniqueness', 'data_integrity']
    )

def create_range_rule(column: str, min_value: Optional[float] = None, 
                     max_value: Optional[float] = None) -> ValidationRule:
    """Create numeric range validation rule"""
    
    def validate_range(df: pd.DataFrame) -> ValidationResult:
        if column not in df.columns:
            return ValidationResult(
                rule_name=f"range_{column}",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message=f"Column '{column}' not found"
            )
        
        # Filter out null values for range checking
        non_null_series = df[column].dropna()
        
        violations = pd.Series([False] * len(non_null_series))
        
        if min_value is not None:
            violations |= (non_null_series < min_value)
        
        if max_value is not None:
            violations |= (non_null_series > max_value)
        
        violation_count = violations.sum()
        passed = violation_count == 0
        
        range_desc = f"[{min_value}, {max_value}]"
        
        return ValidationResult(
            rule_name=f"range_{column}",
            severity=ValidationSeverity.ERROR,
            passed=passed,
            message=f"Column '{column}' has {violation_count} values outside range {range_desc}",
            details={
                'min_value': min_value,
                'max_value': max_value,
                'violations': violation_count,
                'total_checked': len(non_null_series)
            },
            affected_rows=violation_count
        )
    
    return ValidationRule(
        name=f"range_{column}",
        description=f"Validates that column '{column}' values are within specified range",
        validation_func=validate_range,
        tags=['range', 'data_quality']
    )

# 2. Advanced Data Transformation System
class TransformationStep(BaseModel):
    """Single transformation step configuration"""
    name: str
    description: str
    function: str  # Function name or lambda expression
    parameters: Dict[str, Any] = Field(default_factory=dict)
    condition: Optional[str] = None  # Conditional execution
    error_handling: str = "raise"  # 'raise', 'skip', 'log'

class DataTransformationPipeline:
    """Advanced data transformation pipeline"""
    
    def __init__(self):
        self.steps: List[TransformationStep] = []
        self.transformation_log: List[Dict[str, Any]] = []
        self.custom_functions: Dict[str, Callable] = {}
    
    def register_function(self, name: str, func: Callable):
        """Register custom transformation function"""
        self.custom_functions[name] = func
        logging.info(f"Registered custom transformation function: {name}")
    
    def add_step(self, step: TransformationStep):
        """Add transformation step to pipeline"""
        self.steps.append(step)
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Execute all transformation steps"""
        result_df = df.copy()
        
        for i, step in enumerate(self.steps):
            start_time = datetime.now()
            
            try:
                # Check condition if specified
                if step.condition:
                    if not self._evaluate_condition(step.condition, result_df):
                        logging.info(f"Skipping step '{step.name}' - condition not met")
                        continue
                
                # Execute transformation
                logging.info(f"Executing transformation step: {step.name}")
                result_df = self._execute_step(step, result_df)
                
                # Log successful execution
                execution_time = (datetime.now() - start_time).total_seconds()
                self.transformation_log.append({
                    'step_name': step.name,
                    'step_index': i,
                    'status': 'success',
                    'execution_time': execution_time,
                    'rows_before': len(df),
                    'rows_after': len(result_df),
                    'timestamp': start_time.isoformat()
                })
                
            except Exception as e:
                execution_time = (datetime.now() - start_time).total_seconds()
                error_log = {
                    'step_name': step.name,
                    'step_index': i,
                    'status': 'error',
                    'error_message': str(e),
                    'execution_time': execution_time,
                    'timestamp': start_time.isoformat()
                }
                
                if step.error_handling == 'raise':
                    self.transformation_log.append(error_log)
                    raise
                elif step.error_handling == 'skip':
                    logging.warning(f"Skipping failed step '{step.name}': {str(e)}")
                    self.transformation_log.append({**error_log, 'action': 'skipped'})
                elif step.error_handling == 'log':
                    logging.error(f"Step '{step.name}' failed but continuing: {str(e)}")
                    self.transformation_log.append({**error_log, 'action': 'logged'})
        
        return result_df
    
    def _execute_step(self, step: TransformationStep, df: pd.DataFrame) -> pd.DataFrame:
        """Execute single transformation step"""
        
        # Check if it's a registered custom function
        if step.function in self.custom_functions:
            func = self.custom_functions[step.function]
            return func(df, **step.parameters)
        
        # Check if it's a pandas DataFrame method
        if hasattr(df, step.function):
            method = getattr(df, step.function)
            return method(**step.parameters)
        
        # Try to evaluate as lambda expression
        if step.function.startswith('lambda'):
            func = eval(step.function)
            return func(df, **step.parameters)
        
        raise ValueError(f"Unknown transformation function: {step.function}")
    
    def _evaluate_condition(self, condition: str, df: pd.DataFrame) -> bool:
        """Evaluate conditional expression"""
        try:
            # Simple condition evaluation (could be enhanced with ast for safety)
            return eval(condition, {'df': df, 'len': len, 'pd': pd, 'np': np})
        except Exception as e:
            logging.error(f"Failed to evaluate condition '{condition}': {str(e)}")
            return False

# Pre-built transformation functions
def standardize_email(df: pd.DataFrame, email_column: str = 'email') -> pd.DataFrame:
    """Standardize email addresses"""
    result_df = df.copy()
    if email_column in result_df.columns:
        result_df[email_column] = (
            result_df[email_column]
            .astype(str)
            .str.lower()
            .str.strip()
        )
        
        # Mark invalid emails
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        result_df[f'{email_column}_valid'] = result_df[email_column].str.match(email_pattern)
    
    return result_df

def calculate_customer_metrics(df: pd.DataFrame, customer_id_col: str = 'customer_id',
                             order_amount_col: str = 'order_amount',
                             order_date_col: str = 'order_date') -> pd.DataFrame:
    """Calculate customer-level metrics"""
    
    metrics = df.groupby(customer_id_col).agg({
        order_amount_col: ['count', 'sum', 'mean', 'std'],
        order_date_col: ['min', 'max']
    }).round(2)
    
    # Flatten column names
    metrics.columns = [f'{col[1]}_{col[0]}' if col[1] else col[0] for col in metrics.columns]
    
    # Calculate additional metrics
    metrics['days_active'] = (metrics[f'max_{order_date_col}'] - metrics[f'min_{order_date_col}']).dt.days
    metrics['avg_days_between_orders'] = metrics['days_active'] / metrics[f'count_{order_amount_col}']
    
    # Reset index to convert customer_id back to column
    return metrics.reset_index()

# 3. Advanced Data Loading System
class LoadingStrategy(Enum):
    APPEND = "append"
    REPLACE = "replace"
    UPSERT = "upsert"
    MERGE = "merge"

@dataclass
class LoadingConfig:
    """Data loading configuration"""
    target_table: str
    strategy: LoadingStrategy
    batch_size: int = 1000
    unique_key: Optional[List[str]] = None
    update_columns: Optional[List[str]] = None
    create_table: bool = True
    add_metadata: bool = True
    schema_validation: bool = True

class AdvancedDataLoader:
    """Advanced data loading with multiple strategies and error handling"""
    
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        self.metadata = MetaData(bind=self.engine)
        self.load_history: List[Dict[str, Any]] = []
    
    def load_dataframe(self, df: pd.DataFrame, config: LoadingConfig) -> Dict[str, Any]:
        """Load DataFrame using specified strategy"""
        
        start_time = datetime.now()
        load_id = self._generate_load_id()
        
        try:
            # Add metadata columns if requested
            if config.add_metadata:
                df = self._add_metadata_columns(df, load_id)
            
            # Validate schema if requested
            if config.schema_validation:
                self._validate_schema(df, config.target_table)
            
            # Execute loading strategy
            if config.strategy == LoadingStrategy.APPEND:
                result = self._append_data(df, config)
            elif config.strategy == LoadingStrategy.REPLACE:
                result = self._replace_data(df, config)
            elif config.strategy == LoadingStrategy.UPSERT:
                result = self._upsert_data(df, config)
            elif config.strategy == LoadingStrategy.MERGE:
                result = self._merge_data(df, config)
            else:
                raise ValueError(f"Unknown loading strategy: {config.strategy}")
            
            # Log successful load
            execution_time = (datetime.now() - start_time).total_seconds()
            load_log = {
                'load_id': load_id,
                'target_table': config.target_table,
                'strategy': config.strategy.value,
                'rows_loaded': len(df),
                'execution_time': execution_time,
                'status': 'success',
                'timestamp': start_time.isoformat(),
                **result
            }
            
            self.load_history.append(load_log)
            logging.info(f"Successfully loaded {len(df)} rows to {config.target_table}")
            
            return load_log
            
        except Exception as e:
            # Log failed load
            execution_time = (datetime.now() - start_time).total_seconds()
            error_log = {
                'load_id': load_id,
                'target_table': config.target_table,
                'strategy': config.strategy.value,
                'rows_attempted': len(df),
                'execution_time': execution_time,
                'status': 'error',
                'error_message': str(e),
                'timestamp': start_time.isoformat()
            }
            
            self.load_history.append(error_log)
            logging.error(f"Failed to load data to {config.target_table}: {str(e)}")
            raise
    
    def _append_data(self, df: pd.DataFrame, config: LoadingConfig) -> Dict[str, Any]:
        """Append data to existing table"""
        
        rows_inserted = 0
        for batch_start in range(0, len(df), config.batch_size):
            batch_end = min(batch_start + config.batch_size, len(df))
            batch_df = df.iloc[batch_start:batch_end]
            
            batch_df.to_sql(
                config.target_table,
                self.engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            rows_inserted += len(batch_df)
            logging.debug(f"Inserted batch: {batch_start}-{batch_end} ({len(batch_df)} rows)")
        
        return {'rows_inserted': rows_inserted, 'rows_updated': 0, 'rows_deleted': 0}
    
    def _replace_data(self, df: pd.DataFrame, config: LoadingConfig) -> Dict[str, Any]:
        """Replace all data in table"""
        
        # Get row count before replacement
        try:
            with self.engine.connect() as conn:
                result = conn.execute(f"SELECT COUNT(*) FROM {config.target_table}")
                rows_before = result.fetchone()[0]
        except:
            rows_before = 0
        
        # Replace data
        df.to_sql(
            config.target_table,
            self.engine,
            if_exists='replace',
            index=False,
            method='multi'
        )
        
        return {
            'rows_inserted': len(df),
            'rows_updated': 0,
            'rows_deleted': rows_before
        }
    
    def _upsert_data(self, df: pd.DataFrame, config: LoadingConfig) -> Dict[str, Any]:
        """Upsert data (insert new, update existing)"""
        
        if not config.unique_key:
            raise ValueError("Unique key required for upsert strategy")
        
        # This is a simplified implementation
        # Production would use database-specific MERGE/ON CONFLICT syntax
        
        rows_inserted = 0
        rows_updated = 0
        
        # Check existing records
        unique_key_str = ', '.join(config.unique_key)
        existing_query = f"SELECT {unique_key_str} FROM {config.target_table}"
        
        try:
            existing_df = pd.read_sql(existing_query, self.engine)
            existing_keys = set(existing_df[config.unique_key].apply(tuple, axis=1))
        except:
            existing_keys = set()
        
        # Split into insert and update batches
        df_keys = df[config.unique_key].apply(tuple, axis=1)
        insert_mask = ~df_keys.isin(existing_keys)
        update_mask = df_keys.isin(existing_keys)
        
        # Insert new records
        if insert_mask.any():
            insert_df = df[insert_mask]
            insert_df.to_sql(
                config.target_table,
                self.engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            rows_inserted = len(insert_df)
        
        # Update existing records (simplified - would use proper UPDATE statements)
        if update_mask.any():
            rows_updated = update_mask.sum()
        
        return {
            'rows_inserted': rows_inserted,
            'rows_updated': rows_updated,
            'rows_deleted': 0
        }
    
    def _merge_data(self, df: pd.DataFrame, config: LoadingConfig) -> Dict[str, Any]:
        """Merge data using custom logic"""
        # Implementation would depend on specific merge requirements
        return self._upsert_data(df, config)
    
    def _add_metadata_columns(self, df: pd.DataFrame, load_id: str) -> pd.DataFrame:
        """Add metadata columns for tracking"""
        result_df = df.copy()
        result_df['_load_id'] = load_id
        result_df['_loaded_at'] = datetime.now()
        result_df['_source_rows'] = len(df)
        return result_df
    
    def _validate_schema(self, df: pd.DataFrame, table_name: str):
        """Validate DataFrame schema against target table"""
        # Implementation would check column types, constraints, etc.
        pass
    
    def _generate_load_id(self) -> str:
        """Generate unique load identifier"""
        timestamp = datetime.now().isoformat()
        return hashlib.md5(timestamp.encode()).hexdigest()[:8]

# 4. Complete ETL Pipeline Integration
class ETLPipeline:
    """Complete ETL pipeline with validation, transformation, and loading"""
    
    def __init__(self, name: str):
        self.name = name
        self.validator = AdvancedDataValidator()
        self.transformer = DataTransformationPipeline()
        self.loader: Optional[AdvancedDataLoader] = None
        self.execution_log: List[Dict[str, Any]] = []
    
    def set_loader(self, connection_string: str):
        """Set up data loader"""
        self.loader = AdvancedDataLoader(connection_string)
    
    def execute(self, source_data: pd.DataFrame, 
               loading_config: Optional[LoadingConfig] = None,
               validation_tags: Optional[List[str]] = None) -> Dict[str, Any]:
        """Execute complete ETL pipeline"""
        
        pipeline_start = datetime.now()
        execution_id = hashlib.md5(f"{self.name}_{pipeline_start.isoformat()}".encode()).hexdigest()[:8]
        
        logging.info(f"Starting ETL pipeline '{self.name}' with execution ID: {execution_id}")
        
        try:
            # Stage 1: Validation
            logging.info("Stage 1: Data Validation")
            validation_results = self.validator.validate_dataframe(source_data, validation_tags)
            validation_summary = self.validator.get_validation_summary(validation_results)
            
            # Check if validation passed
            validation_errors = [r for r in validation_results 
                               if r.severity == ValidationSeverity.ERROR and not r.passed]
            
            if validation_errors:
                raise ValueError(f"Validation failed with {len(validation_errors)} errors")
            
            # Stage 2: Transformation
            logging.info("Stage 2: Data Transformation")
            transformed_data = self.transformer.transform(source_data)
            
            # Stage 3: Loading
            load_result = {}
            if loading_config and self.loader:
                logging.info("Stage 3: Data Loading")
                load_result = self.loader.load_dataframe(transformed_data, loading_config)
            
            # Pipeline completion
            pipeline_duration = (datetime.now() - pipeline_start).total_seconds()
            
            execution_summary = {
                'execution_id': execution_id,
                'pipeline_name': self.name,
                'status': 'success',
                'pipeline_duration': pipeline_duration,
                'input_rows': len(source_data),
                'output_rows': len(transformed_data),
                'validation_summary': validation_summary,
                'transformation_steps': len(self.transformer.steps),
                'load_result': load_result,
                'timestamp': pipeline_start.isoformat()
            }
            
            self.execution_log.append(execution_summary)
            logging.info(f"ETL pipeline '{self.name}' completed successfully in {pipeline_duration:.2f}s")
            
            return execution_summary
            
        except Exception as e:
            pipeline_duration = (datetime.now() - pipeline_start).total_seconds()
            
            error_summary = {
                'execution_id': execution_id,
                'pipeline_name': self.name,
                'status': 'error',
                'error_message': str(e),
                'pipeline_duration': pipeline_duration,
                'input_rows': len(source_data),
                'timestamp': pipeline_start.isoformat()
            }
            
            self.execution_log.append(error_summary)
            logging.error(f"ETL pipeline '{self.name}' failed after {pipeline_duration:.2f}s: {str(e)}")
            
            raise

# 5. Usage Example
def create_customer_analytics_pipeline():
    """Example: Create comprehensive customer analytics pipeline"""
    
    # Create pipeline
    pipeline = ETLPipeline("customer_analytics")
    
    # Add validation rules
    pipeline.validator.add_rule(create_completeness_rule('customer_id', 0.99))
    pipeline.validator.add_rule(create_completeness_rule('email', 0.95))
    pipeline.validator.add_rule(create_uniqueness_rule('customer_id'))
    pipeline.validator.add_rule(create_range_rule('order_amount', min_value=0, max_value=10000))
    
    # Register custom transformation functions
    pipeline.transformer.register_function('standardize_email', standardize_email)
    pipeline.transformer.register_function('calculate_customer_metrics', calculate_customer_metrics)
    
    # Add transformation steps
    pipeline.transformer.add_step(TransformationStep(
        name="standardize_email_addresses",
        description="Clean and standardize email addresses",
        function="standardize_email",
        parameters={"email_column": "email"}
    ))
    
    pipeline.transformer.add_step(TransformationStep(
        name="calculate_metrics",
        description="Calculate customer-level metrics",
        function="calculate_customer_metrics",
        parameters={
            "customer_id_col": "customer_id",
            "order_amount_col": "order_amount",
            "order_date_col": "order_date"
        }
    ))
    
    # Set up loader (would use real connection string)
    # pipeline.set_loader("postgresql://user:pass@localhost/analytics")
    
    return pipeline

# Example usage
if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Create sample data
    sample_data = pd.DataFrame({
        'customer_id': ['CUST001', 'CUST002', 'CUST003', 'CUST001'],
        'email': ['user1@EXAMPLE.COM ', 'user2@example.com', 'invalid-email', 'user1@example.com'],
        'order_amount': [150.0, 75.0, 200.0, 125.0],
        'order_date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-15'])
    })
    
    # Create and execute pipeline
    pipeline = create_customer_analytics_pipeline()
    
    # Execute without loading (for demonstration)
    result = pipeline.execute(sample_data, validation_tags=['data_quality'])
    
    print(f"Pipeline execution result: {json.dumps(result, indent=2, default=str)}")
```

**System Capabilities**:
- **Advanced Validation**: Multi-level validation with configurable severity levels
- **Flexible Transformation**: Pipeline-based transformations with error handling
- **Multiple Loading Strategies**: Append, replace, upsert, and merge patterns
- **Comprehensive Logging**: Detailed execution tracking and performance monitoring
- **Error Recovery**: Configurable error handling strategies
- **Schema Management**: Automatic schema validation and evolution
- **Batch Processing**: Efficient processing of large datasets
- **Metadata Tracking**: Complete audit trail of data lineage

This comprehensive system provides production-ready data processing capabilities suitable for enterprise Analytics Engineering workflows.

---

These Python questions demonstrate mastery of:
- **Advanced data structures** and performance optimization patterns
- **Object-oriented design** with SOLID principles and design patterns
- **Robust API integration** with comprehensive error handling and rate limiting
- **Advanced Python features** including decorators, context managers, and async programming
- **Production-ready data pipeline** development with validation, transformation, and loading

Each question includes practical, real-world examples that show hands-on experience with enterprise-scale Analytics Engineering challenges using Python.