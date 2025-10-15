# Python Questions for Analytics Engineers

## 1. Data Structures & Algorithms for AE

**Q1: Implement an efficient data pipeline processor that handles nested JSON transformations.**

**Answer:**
```python
import json
from typing import Dict, Any, List, Union
from functools import reduce
import logging

class JSONDataProcessor:
    """Efficient processor for complex nested JSON transformations"""
    
    def __init__(self):
        self.transformation_cache = {}
        self.logger = logging.getLogger(__name__)
    
    def flatten_nested_dict(self, data: Dict, parent_key: str = '', sep: str = '_') -> Dict:
        """Recursively flatten nested dictionaries"""
        items = []
        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            
            if isinstance(value, dict):
                items.extend(self.flatten_nested_dict(value, new_key, sep).items())
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # Handle list of dictionaries
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        items.extend(
                            self.flatten_nested_dict(item, f"{new_key}{sep}{i}", sep).items()
                        )
                    else:
                        items.append((f"{new_key}{sep}{i}", item))
            else:
                items.append((new_key, value))
        
        return dict(items)
    
    def apply_transformations(self, data: List[Dict], transformations: Dict[str, callable]) -> List[Dict]:
        """Apply multiple transformations efficiently using map/filter patterns"""
        
        def transform_record(record: Dict) -> Dict:
            flattened = self.flatten_nested_dict(record)
            
            # Apply transformations
            for field, transform_func in transformations.items():
                if field in flattened:
                    try:
                        flattened[field] = transform_func(flattened[field])
                    except Exception as e:
                        self.logger.warning(f"Transformation failed for {field}: {e}")
                        flattened[f"{field}_error"] = str(e)
            
            return flattened
        
        # Use map for efficient transformation
        return list(map(transform_record, data))
    
    def batch_process(self, data_stream, batch_size: int = 1000) -> List[Dict]:
        """Process data in batches for memory efficiency"""
        processed_batches = []
        
        def process_batch(batch):
            # Simulate complex transformations
            transformations = {
                'amount': lambda x: float(x) if x else 0.0,
                'timestamp': lambda x: pd.to_datetime(x).isoformat(),
                'email': lambda x: x.lower().strip() if isinstance(x, str) else x
            }
            return self.apply_transformations(batch, transformations)
        
        # Process in batches
        batch = []
        for record in data_stream:
            batch.append(record)
            if len(batch) >= batch_size:
                processed_batches.extend(process_batch(batch))
                batch = []
        
        # Process remaining records
        if batch:
            processed_batches.extend(process_batch(batch))
        
        return processed_batches

# Usage example
processor = JSONDataProcessor()
sample_data = [
    {
        "user": {
            "id": 123,
            "profile": {
                "name": "John Doe",
                "email": "JOHN@EXAMPLE.COM"
            }
        },
        "orders": [
            {"id": 1, "amount": "99.99"},
            {"id": 2, "amount": "149.50"}
        ]
    }
]

processed = processor.batch_process(sample_data)
print(json.dumps(processed[0], indent=2))
```

**Q2: Design a robust data validation framework using decorators and context managers.**

**Answer:**
```python
import functools
import pandas as pd
from typing import Callable, Any, Dict, List
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
import logging

class ValidationSeverity(Enum):
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class ValidationResult:
    passed: bool
    message: str
    severity: ValidationSeverity
    field: str = None
    failed_records: List[Any] = None

class DataValidationError(Exception):
    def __init__(self, validations: List[ValidationResult]):
        self.validations = validations
        failed_count = sum(1 for v in validations if not v.passed)
        super().__init__(f"Data validation failed: {failed_count} checks failed")

def validate_data(
    checks: List[Callable] = None,
    on_failure: str = "raise",  # "raise", "warn", "continue"
    return_results: bool = False
):
    """Decorator for comprehensive data validation"""
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Execute the function first
            result = func(*args, **kwargs)
            
            if not isinstance(result, pd.DataFrame):
                return result
            
            validation_results = []
            
            # Run validation checks
            for check in (checks or []):
                try:
                    validation_result = check(result)
                    validation_results.append(validation_result)
                except Exception as e:
                    validation_results.append(
                        ValidationResult(
                            passed=False,
                            message=f"Validation check failed: {str(e)}",
                            severity=ValidationSeverity.ERROR
                        )
                    )
            
            # Handle failures based on strategy
            failed_validations = [v for v in validation_results if not v.passed]
            
            if failed_validations:
                if on_failure == "raise":
                    raise DataValidationError(failed_validations)
                elif on_failure == "warn":
                    for validation in failed_validations:
                        logging.warning(f"Validation warning: {validation.message}")
                # Continue processing regardless
            
            if return_results:
                return result, validation_results
            return result
        
        return wrapper
    return decorator

# Validation check functions
def check_no_nulls(column: str, severity: ValidationSeverity = ValidationSeverity.ERROR):
    """Check for null values in specified column"""
    def validator(df: pd.DataFrame) -> ValidationResult:
        null_count = df[column].isnull().sum()
        passed = null_count == 0
        
        return ValidationResult(
            passed=passed,
            message=f"Found {null_count} null values in {column}",
            severity=severity,
            field=column,
            failed_records=df[df[column].isnull()].index.tolist() if not passed else []
        )
    return validator

def check_range(column: str, min_val: float, max_val: float):
    """Check if values are within specified range"""
    def validator(df: pd.DataFrame) -> ValidationResult:
        out_of_range = df[(df[column] < min_val) | (df[column] > max_val)]
        passed = len(out_of_range) == 0
        
        return ValidationResult(
            passed=passed,
            message=f"Found {len(out_of_range)} values outside range [{min_val}, {max_val}] in {column}",
            severity=ValidationSeverity.ERROR,
            field=column,
            failed_records=out_of_range.index.tolist()
        )
    return validator

def check_unique(column: str):
    """Check for duplicate values"""
    def validator(df: pd.DataFrame) -> ValidationResult:
        duplicate_count = df[column].duplicated().sum()
        passed = duplicate_count == 0
        
        return ValidationResult(
            passed=passed,
            message=f"Found {duplicate_count} duplicate values in {column}",
            severity=ValidationSeverity.ERROR,
            field=column
        )
    return validator

@contextmanager
def data_processing_context(log_level: str = "INFO"):
    """Context manager for data processing with logging and error handling"""
    logger = logging.getLogger("data_processing")
    logger.setLevel(getattr(logging, log_level))
    
    logger.info("Starting data processing context")
    start_time = pd.Timestamp.now()
    
    try:
        yield logger
    except DataValidationError as e:
        logger.error(f"Data validation failed: {len(e.validations)} checks failed")
        for validation in e.validations:
            logger.error(f"  - {validation.message}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during data processing: {str(e)}")
        raise
    finally:
        end_time = pd.Timestamp.now()
        duration = end_time - start_time
        logger.info(f"Data processing completed in {duration.total_seconds():.2f} seconds")

# Usage example
@validate_data(
    checks=[
        check_no_nulls('customer_id'),
        check_range('amount', 0, 10000),
        check_unique('order_id')
    ],
    on_failure="raise"
)
def process_orders_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process orders data with built-in validation"""
    # Data processing logic
    df['amount_usd'] = df['amount'] * df.get('exchange_rate', 1.0)
    df['processed_at'] = pd.Timestamp.now()
    
    return df

# Usage with context manager
with data_processing_context() as logger:
    orders_df = pd.DataFrame({
        'order_id': [1, 2, 3, 4],
        'customer_id': [101, 102, 103, None],  # Will trigger validation error
        'amount': [99.99, 149.50, -10, 250.00]  # Negative amount will trigger error
    })
    
    try:
        processed_df = process_orders_data(orders_df)
        logger.info(f"Successfully processed {len(processed_df)} orders")
    except DataValidationError as e:
        logger.error("Data validation failed - implementing fallback processing")
```

## 2. Advanced Pandas & Data Manipulation

**Q3: Implement memory-efficient data processing for large datasets using chunking and optimization techniques.**

**Answer:**
```python
import pandas as pd
import numpy as np
from typing import Iterator, Callable, Optional, Union
import gc
from functools import reduce
import dask.dataframe as dd

class OptimizedDataProcessor:
    """Memory-efficient data processing for large datasets"""
    
    def __init__(self, chunk_size: int = 10000):
        self.chunk_size = chunk_size
        self.dtype_optimizations = {
            'int64': 'int32',
            'float64': 'float32',
            'object': 'category'  # For string columns with limited unique values
        }
    
    def optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize DataFrame memory usage by converting data types"""
        optimized_df = df.copy()
        
        for column in optimized_df.columns:
            col_type = str(optimized_df[column].dtype)
            
            # Optimize numeric types
            if col_type == 'int64':
                if optimized_df[column].min() >= np.iinfo(np.int8).min and \
                   optimized_df[column].max() <= np.iinfo(np.int8).max:
                    optimized_df[column] = optimized_df[column].astype('int8')
                elif optimized_df[column].min() >= np.iinfo(np.int16).min and \
                     optimized_df[column].max() <= np.iinfo(np.int16).max:
                    optimized_df[column] = optimized_df[column].astype('int16')
                elif optimized_df[column].min() >= np.iinfo(np.int32).min and \
                     optimized_df[column].max() <= np.iinfo(np.int32).max:
                    optimized_df[column] = optimized_df[column].astype('int32')
            
            elif col_type == 'float64':
                # Check if we can use float32 without losing precision
                temp_col = optimized_df[column].astype('float32')
                if np.allclose(optimized_df[column].dropna(), temp_col.dropna(), equal_nan=True):
                    optimized_df[column] = temp_col
            
            elif col_type == 'object':
                # Convert to category if unique values < 50% of total
                unique_ratio = len(optimized_df[column].unique()) / len(optimized_df)
                if unique_ratio < 0.5:
                    optimized_df[column] = optimized_df[column].astype('category')
        
        return optimized_df
    
    def chunked_processing(
        self, 
        file_path: str, 
        processing_func: Callable[[pd.DataFrame], pd.DataFrame],
        output_path: Optional[str] = None,
        **read_kwargs
    ) -> Union[pd.DataFrame, None]:
        """Process large files in chunks"""
        
        processed_chunks = []
        total_rows = 0
        
        # Read and process in chunks
        chunk_iter = pd.read_csv(file_path, chunksize=self.chunk_size, **read_kwargs)
        
        for i, chunk in enumerate(chunk_iter):
            print(f"Processing chunk {i+1}...")
            
            # Optimize memory usage
            chunk = self.optimize_dtypes(chunk)
            
            # Apply processing function
            processed_chunk = processing_func(chunk)
            
            # Handle output
            if output_path:
                # Write to file (append mode after first chunk)
                mode = 'w' if i == 0 else 'a'
                header = i == 0
                processed_chunk.to_csv(output_path, mode=mode, header=header, index=False)
            else:
                processed_chunks.append(processed_chunk)
            
            total_rows += len(chunk)
            
            # Force garbage collection
            del chunk, processed_chunk
            gc.collect()
        
        print(f"Processed {total_rows} total rows")
        
        if not output_path:
            # Combine all chunks
            result = pd.concat(processed_chunks, ignore_index=True)
            return self.optimize_dtypes(result)
        
        return None
    
    def parallel_aggregation(
        self, 
        df: pd.DataFrame, 
        group_cols: list, 
        agg_funcs: dict,
        n_partitions: int = 4
    ) -> pd.DataFrame:
        """Perform aggregations using Dask for parallelization"""
        
        # Convert to Dask DataFrame
        ddf = dd.from_pandas(df, npartitions=n_partitions)
        
        # Perform aggregation
        result = ddf.groupby(group_cols).agg(agg_funcs)
        
        # Compute and return
        return result.compute()
    
    def rolling_window_operations(
        self, 
        df: pd.DataFrame, 
        date_col: str, 
        value_col: str,
        window_sizes: list = [7, 30, 90]
    ) -> pd.DataFrame:
        """Efficient rolling window calculations"""
        
        # Ensure datetime index
        if df[date_col].dtype != 'datetime64[ns]':
            df[date_col] = pd.to_datetime(df[date_col])
        
        df_sorted = df.sort_values(date_col)
        
        # Calculate multiple rolling windows efficiently
        for window in window_sizes:
            df_sorted[f'{value_col}_rolling_{window}d'] = (
                df_sorted.set_index(date_col)[value_col]
                .rolling(f'{window}D')
                .mean()
                .values
            )
        
        return df_sorted.reset_index(drop=True)

# Example usage
processor = OptimizedDataProcessor(chunk_size=5000)

def data_transformation(chunk_df: pd.DataFrame) -> pd.DataFrame:
    """Example transformation function"""
    # Add calculated columns
    chunk_df['total_amount'] = chunk_df['quantity'] * chunk_df['price']
    chunk_df['discount_amount'] = chunk_df['total_amount'] * chunk_df.get('discount_rate', 0)
    chunk_df['final_amount'] = chunk_df['total_amount'] - chunk_df['discount_amount']
    
    # Filter valid records
    chunk_df = chunk_df[chunk_df['final_amount'] > 0]
    
    return chunk_df

# Process large file efficiently
# result = processor.chunked_processing(
#     'large_sales_data.csv',
#     data_transformation,
#     output_path='processed_sales_data.csv'
# )
```

**Q4: Implement advanced time-series analysis functions for analytics workloads.**

**Answer:**
```python
import pandas as pd
import numpy as np
from typing import List, Tuple, Optional, Dict
from datetime import datetime, timedelta
from scipy import stats
import warnings

class TimeSeriesAnalyzer:
    """Advanced time series analysis for analytics engineering"""
    
    def __init__(self):
        self.seasonality_cache = {}
    
    def detect_outliers(
        self, 
        df: pd.DataFrame, 
        date_col: str, 
        value_col: str,
        method: str = 'iqr',
        threshold: float = 1.5
    ) -> pd.DataFrame:
        """Detect outliers in time series data using multiple methods"""
        
        df_copy = df.copy()
        df_copy[date_col] = pd.to_datetime(df_copy[date_col])
        df_copy = df_copy.sort_values(date_col)
        
        if method == 'iqr':
            Q1 = df_copy[value_col].quantile(0.25)
            Q3 = df_copy[value_col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            
            df_copy['is_outlier'] = (
                (df_copy[value_col] < lower_bound) | 
                (df_copy[value_col] > upper_bound)
            )
            
        elif method == 'zscore':
            z_scores = np.abs(stats.zscore(df_copy[value_col]))
            df_copy['is_outlier'] = z_scores > threshold
            df_copy['z_score'] = z_scores
            
        elif method == 'rolling_zscore':
            # Rolling z-score for time-aware outlier detection
            rolling_mean = df_copy[value_col].rolling(window=30, min_periods=5).mean()
            rolling_std = df_copy[value_col].rolling(window=30, min_periods=5).std()
            
            z_scores = np.abs((df_copy[value_col] - rolling_mean) / rolling_std)
            df_copy['is_outlier'] = z_scores > threshold
            df_copy['rolling_z_score'] = z_scores
        
        return df_copy
    
    def seasonal_decomposition(
        self, 
        df: pd.DataFrame, 
        date_col: str, 
        value_col: str,
        freq: str = 'D',
        model: str = 'additive'
    ) -> Dict[str, pd.Series]:
        """Perform seasonal decomposition of time series"""
        
        # Prepare data
        df_ts = df.set_index(pd.to_datetime(df[date_col]))[value_col]
        df_ts = df_ts.resample(freq).sum()  # Aggregate by frequency
        
        # Simple seasonal decomposition
        if freq == 'D':
            seasonal_period = 7  # Weekly seasonality
        elif freq == 'M':
            seasonal_period = 12  # Monthly seasonality
        else:
            seasonal_period = 4  # Quarterly
        
        # Calculate trend using moving average
        trend = df_ts.rolling(window=seasonal_period, center=True).mean()
        
        # Calculate seasonal component
        detrended = df_ts - trend
        seasonal = detrended.groupby(detrended.index.dayofyear if freq == 'D' 
                                   else detrended.index.month).transform('mean')
        
        # Calculate residual
        if model == 'additive':
            residual = df_ts - trend - seasonal
        else:  # multiplicative
            residual = df_ts / (trend * seasonal)
        
        return {
            'original': df_ts,
            'trend': trend,
            'seasonal': seasonal,
            'residual': residual
        }
    
    def calculate_growth_metrics(
        self, 
        df: pd.DataFrame, 
        date_col: str, 
        value_col: str,
        periods: List[int] = [1, 7, 30, 365]
    ) -> pd.DataFrame:
        """Calculate various growth metrics"""
        
        df_copy = df.copy()
        df_copy[date_col] = pd.to_datetime(df_copy[date_col])
        df_copy = df_copy.sort_values(date_col)
        
        for period in periods:
            # Period-over-period growth
            df_copy[f'growth_{period}d_abs'] = (
                df_copy[value_col] - df_copy[value_col].shift(period)
            )
            
            df_copy[f'growth_{period}d_pct'] = (
                df_copy[value_col].pct_change(periods=period) * 100
            )
            
            # Compound Annual Growth Rate (for longer periods)
            if period >= 365:
                years = period / 365
                df_copy[f'cagr_{period}d'] = (
                    ((df_copy[value_col] / df_copy[value_col].shift(period)) ** (1/years) - 1) * 100
                )
        
        return df_copy
    
    def cohort_analysis(
        self, 
        df: pd.DataFrame,
        user_col: str,
        date_col: str,
        value_col: Optional[str] = None
    ) -> pd.DataFrame:
        """Perform cohort analysis for user retention/revenue"""
        
        df_copy = df.copy()
        df_copy[date_col] = pd.to_datetime(df_copy[date_col])
        
        # Define cohort month (first purchase month for each user)
        df_copy['order_period'] = df_copy[date_col].dt.to_period('M')
        
        cohort_data = df_copy.groupby(user_col)[date_col].min().reset_index()
        cohort_data.columns = [user_col, 'cohort_month']
        cohort_data['cohort_month'] = cohort_data['cohort_month'].dt.to_period('M')
        
        # Merge back to get cohort month for each transaction
        df_cohort = df_copy.merge(cohort_data, on=user_col)
        df_cohort['period_number'] = (
            df_cohort['order_period'] - df_cohort['cohort_month']
        ).apply(attrgetter('n'))
        
        if value_col:
            # Revenue cohort analysis
            cohort_table = df_cohort.groupby(['cohort_month', 'period_number'])[value_col].sum().reset_index()
        else:
            # User retention cohort analysis
            cohort_table = df_cohort.groupby(['cohort_month', 'period_number'])[user_col].nunique().reset_index()
        
        # Pivot to create cohort matrix
        cohort_matrix = cohort_table.pivot(index='cohort_month', 
                                         columns='period_number', 
                                         values=value_col if value_col else user_col)
        
        # Calculate cohort sizes
        cohort_sizes = df_cohort.groupby('cohort_month')[user_col].nunique()
        
        # Calculate retention rates
        if not value_col:
            retention_matrix = cohort_matrix.divide(cohort_sizes, axis=0)
            return retention_matrix
        
        return cohort_matrix
    
    def forecast_simple(
        self, 
        df: pd.DataFrame, 
        date_col: str, 
        value_col: str,
        periods: int = 30,
        method: str = 'linear'
    ) -> pd.DataFrame:
        """Simple forecasting methods for time series"""
        
        df_copy = df.copy()
        df_copy[date_col] = pd.to_datetime(df_copy[date_col])
        df_copy = df_copy.sort_values(date_col)
        
        # Prepare features for regression
        df_copy['days_since_start'] = (df_copy[date_col] - df_copy[date_col].min()).dt.days
        
        if method == 'linear':
            # Linear regression
            X = df_copy['days_since_start'].values.reshape(-1, 1)
            y = df_copy[value_col].values
            
            from sklearn.linear_model import LinearRegression
            model = LinearRegression()
            model.fit(X, y)
            
            # Generate future dates
            last_date = df_copy[date_col].max()
            future_dates = pd.date_range(start=last_date + timedelta(days=1), periods=periods)
            future_days = [(d - df_copy[date_col].min()).days for d in future_dates]
            
            predictions = model.predict(np.array(future_days).reshape(-1, 1))
            
        elif method == 'exponential_smoothing':
            # Simple exponential smoothing
            alpha = 0.3
            forecast_values = []
            s = df_copy[value_col].iloc[0]  # Initial forecast
            
            for i in range(1, len(df_copy)):
                s = alpha * df_copy[value_col].iloc[i] + (1 - alpha) * s
            
            # Generate forecasts
            predictions = [s] * periods
            future_dates = pd.date_range(start=df_copy[date_col].max() + timedelta(days=1), periods=periods)
        
        # Create forecast DataFrame
        forecast_df = pd.DataFrame({
            date_col: future_dates,
            f'{value_col}_forecast': predictions
        })
        
        return forecast_df

# Example usage
analyzer = TimeSeriesAnalyzer()

# Sample data
sample_data = pd.DataFrame({
    'date': pd.date_range('2023-01-01', periods=365, freq='D'),
    'sales': np.random.normal(1000, 100, 365) + np.sin(np.arange(365) * 2 * np.pi / 7) * 50
})

# Detect outliers
outliers = analyzer.detect_outliers(sample_data, 'date', 'sales', method='rolling_zscore')
print(f"Found {outliers['is_outlier'].sum()} outliers")

# Seasonal decomposition
decomposition = analyzer.seasonal_decomposition(sample_data, 'date', 'sales')
print("Seasonal decomposition completed")

# Growth metrics
growth_metrics = analyzer.calculate_growth_metrics(sample_data, 'date', 'sales')
print("Growth metrics calculated")

# Simple forecast
forecast = analyzer.forecast_simple(sample_data, 'date', 'sales', periods=30)
print(f"Generated forecast for {len(forecast)} days")
```

## 3. Error Handling & Testing

**Q5: Implement comprehensive error handling and logging framework for data pipelines.**

**Answer:**
```python
import logging
import functools
import traceback
import sys
from typing import Any, Callable, Optional, Dict, List
from datetime import datetime
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
import json

class ErrorSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class DataPipelineError:
    timestamp: datetime
    function_name: str
    error_type: str
    error_message: str
    severity: ErrorSeverity
    context: Dict[str, Any] = field(default_factory=dict)
    stack_trace: Optional[str] = None
    retry_count: int = 0

class PipelineErrorCollector:
    """Centralized error collection and analysis"""
    
    def __init__(self):
        self.errors: List[DataPipelineError] = []
        self.error_counts = {}
        self.logger = logging.getLogger("pipeline_errors")
    
    def add_error(self, error: DataPipelineError):
        """Add error to collection"""
        self.errors.append(error)
        
        # Track error frequency
        error_key = f"{error.function_name}:{error.error_type}"
        self.error_counts[error_key] = self.error_counts.get(error_key, 0) + 1
        
        # Log based on severity
        if error.severity in [ErrorSeverity.HIGH, ErrorSeverity.CRITICAL]:
            self.logger.error(f"Pipeline Error: {error.error_message}")
        else:
            self.logger.warning(f"Pipeline Warning: {error.error_message}")
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Generate error summary report"""
        return {
            'total_errors': len(self.errors),
            'errors_by_severity': {
                severity.value: len([e for e in self.errors if e.severity == severity])
                for severity in ErrorSeverity
            },
            'most_common_errors': sorted(
                self.error_counts.items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:5],
            'recent_critical_errors': [
                {
                    'timestamp': e.timestamp.isoformat(),
                    'function': e.function_name,
                    'message': e.error_message
                }
                for e in self.errors[-10:]
                if e.severity == ErrorSeverity.CRITICAL
            ]
        }

# Global error collector instance
error_collector = PipelineErrorCollector()

def pipeline_error_handler(
    severity: ErrorSeverity = ErrorSeverity.MEDIUM,
    retry_attempts: int = 0,
    fallback_value: Any = None,
    raise_on_critical: bool = True
):
    """Decorator for comprehensive pipeline error handling"""
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            last_exception = None
            
            while attempt <= retry_attempts:
                try:
                    result = func(*args, **kwargs)
                    
                    # Log successful retry
                    if attempt > 0:
                        logging.info(f"Function {func.__name__} succeeded on retry {attempt}")
                    
                    return result
                    
                except Exception as e:
                    last_exception = e
                    attempt += 1
                    
                    # Create error record
                    pipeline_error = DataPipelineError(
                        timestamp=datetime.now(),
                        function_name=func.__name__,
                        error_type=type(e).__name__,
                        error_message=str(e),
                        severity=severity,
                        context={
                            'args': str(args)[:200],  # Truncate long arguments
                            'kwargs': {k: str(v)[:100] for k, v in kwargs.items()},
                            'attempt': attempt
                        },
                        stack_trace=traceback.format_exc(),
                        retry_count=attempt - 1
                    )
                    
                    error_collector.add_error(pipeline_error)
                    
                    # If we haven't exhausted retries, continue
                    if attempt <= retry_attempts:
                        logging.warning(
                            f"Attempt {attempt} failed for {func.__name__}: {str(e)}. Retrying..."
                        )
                        continue
                    
                    # All retries exhausted
                    if severity == ErrorSeverity.CRITICAL and raise_on_critical:
                        raise e
                    
                    # Return fallback value or re-raise
                    if fallback_value is not None:
                        logging.warning(
                            f"Using fallback value for {func.__name__} after {attempt-1} retries"
                        )
                        return fallback_value
                    
                    raise e
            
        return wrapper
    return decorator

class DataQualityChecker:
    """Comprehensive data quality validation"""
    
    def __init__(self):
        self.quality_rules = {}
        self.logger = logging.getLogger("data_quality")
    
    def add_rule(self, name: str, check_func: Callable, severity: ErrorSeverity = ErrorSeverity.MEDIUM):
        """Add a data quality rule"""
        self.quality_rules[name] = {
            'func': check_func,
            'severity': severity
        }
    
    def validate_dataframe(self, df, context: str = "unknown") -> Dict[str, Any]:
        """Run all quality checks on a DataFrame"""
        results = {
            'passed': True,
            'checks': {},
            'summary': {
                'total_checks': len(self.quality_rules),
                'passed_checks': 0,
                'failed_checks': 0
            }
        }
        
        for rule_name, rule_config in self.quality_rules.items():
            try:
                check_result = rule_config['func'](df)
                
                results['checks'][rule_name] = {
                    'passed': check_result,
                    'severity': rule_config['severity'].value
                }
                
                if check_result:
                    results['summary']['passed_checks'] += 1
                else:
                    results['summary']['failed_checks'] += 1
                    results['passed'] = False
                    
                    # Log failure
                    self.logger.warning(
                        f"Data quality check '{rule_name}' failed for context '{context}'"
                    )
                    
            except Exception as e:
                results['checks'][rule_name] = {
                    'passed': False,
                    'error': str(e),
                    'severity': ErrorSeverity.HIGH.value
                }
                results['passed'] = False
                results['summary']['failed_checks'] += 1
        
        return results

@contextmanager
def pipeline_monitoring_context(pipeline_name: str, alert_on_errors: bool = True):
    """Context manager for pipeline monitoring and alerting"""
    start_time = datetime.now()
    initial_error_count = len(error_collector.errors)
    
    logging.info(f"Starting pipeline: {pipeline_name}")
    
    try:
        yield error_collector
        
        # Pipeline completed successfully
        execution_time = (datetime.now() - start_time).total_seconds()
        new_errors = len(error_collector.errors) - initial_error_count
        
        logging.info(
            f"Pipeline {pipeline_name} completed in {execution_time:.2f}s "
            f"with {new_errors} new errors"
        )
        
    except Exception as e:
        # Pipeline failed
        execution_time = (datetime.now() - start_time).total_seconds()
        
        logging.error(
            f"Pipeline {pipeline_name} failed after {execution_time:.2f}s: {str(e)}"
        )
        
        if alert_on_errors:
            # Send alert (implementation depends on your alerting system)
            send_pipeline_alert(pipeline_name, str(e), error_collector.get_error_summary())
        
        raise

def send_pipeline_alert(pipeline_name: str, error_message: str, error_summary: Dict):
    """Send alert notification (placeholder implementation)"""
    alert_payload = {
        'pipeline': pipeline_name,
        'error': error_message,
        'summary': error_summary,
        'timestamp': datetime.now().isoformat()
    }
    
    # In real implementation, this would send to Slack, email, PagerDuty, etc.
    logging.critical(f"PIPELINE ALERT: {json.dumps(alert_payload, indent=2)}")

# Example usage
quality_checker = DataQualityChecker()

# Add quality rules
quality_checker.add_rule(
    "no_nulls_in_id", 
    lambda df: df['id'].isnull().sum() == 0,
    ErrorSeverity.CRITICAL
)

quality_checker.add_rule(
    "positive_amounts", 
    lambda df: (df['amount'] > 0).all(),
    ErrorSeverity.HIGH
)

@pipeline_error_handler(
    severity=ErrorSeverity.HIGH,
    retry_attempts=2,
    fallback_value=pd.DataFrame()
)
def process_sales_data(file_path: str) -> pd.DataFrame:
    """Example data processing function with error handling"""
    
    # Simulate potential errors
    if "missing_file" in file_path:
        raise FileNotFoundError(f"File not found: {file_path}")
    
    if "corrupt_data" in file_path:
        raise ValueError("Data corruption detected")
    
    # Mock data processing
    df = pd.DataFrame({
        'id': range(1000),
        'amount': np.random.uniform(10, 1000, 1000),
        'category': np.random.choice(['A', 'B', 'C'], 1000)
    })
    
    # Run quality checks
    quality_results = quality_checker.validate_dataframe(df, "sales_processing")
    
    if not quality_results['passed']:
        raise ValueError(f"Data quality checks failed: {quality_results['summary']}")
    
    return df

# Usage with monitoring context
with pipeline_monitoring_context("daily_sales_pipeline") as monitor:
    try:
        # Process multiple files
        results = []
        for file_path in ["sales_data.csv", "missing_file.csv", "corrupt_data.csv"]:
            result = process_sales_data(file_path)
            results.append(result)
        
        logging.info("All files processed successfully")
        
    except Exception as e:
        logging.error(f"Pipeline processing failed: {e}")
    
    # Print error summary
    error_summary = error_collector.get_error_summary()
    print("Pipeline Error Summary:")
    print(json.dumps(error_summary, indent=2))
```

## 4-50. Additional Advanced Python Questions

**Q6: Implement efficient database connection pooling and query optimization**
**Q7: Advanced concurrency patterns with asyncio for data processing**  
**Q8: Memory profiling and optimization techniques for large datasets**
**Q9: Custom context managers for resource management**
**Q10: Advanced API integration patterns with retry logic and rate limiting**
**Q11: Implement data streaming processors with generators and itertools**
**Q12: Advanced configuration management with environment-specific settings**
**Q13: Custom metaclasses for data model validation**
**Q14: Efficient serialization and deserialization patterns**
**Q15: Advanced regex patterns for data cleaning**
**Q16: Implement distributed processing with multiprocessing/threading**
**Q17: Custom exception hierarchies for different error types**
**Q18: Advanced logging configurations with structured logging**
**Q19: Performance monitoring and profiling decorators**
**Q20: Implement data caching strategies with TTL and invalidation**

[Continuing with questions 21-50 covering topics like:]
- Advanced SQLAlchemy ORM patterns
- Custom data validation frameworks  
- Efficient JSON/XML processing
- Advanced datetime handling and timezone management
- Custom data connectors and adapters
- Memory-efficient data structures
- Advanced testing patterns with pytest
- Performance optimization techniques
- Async programming patterns
- Advanced data transformation pipelines
- Custom middleware for data processing
- Advanced security and encryption
- Monitoring and observability patterns
- Advanced packaging and deployment
- Code quality and style enforcement

---

## Python Best Practices for Analytics Engineers:

1. **Code Organization**: Use proper module structure and package management
2. **Error Handling**: Implement comprehensive error handling with proper logging
3. **Performance**: Profile and optimize for large dataset processing
4. **Testing**: Write unit tests and integration tests for data functions
5. **Documentation**: Use type hints and docstrings consistently
6. **Memory Management**: Understand memory usage patterns for large data
7. **Async Programming**: Use async patterns for I/O bound operations
8. **Security**: Implement proper secret management and data protection

## Key Skills for Senior AE Python Roles:
- Advanced pandas and numpy optimization
- Custom framework and library development
- Performance profiling and optimization
- Complex error handling and monitoring
- Integration patterns with cloud services
- Advanced testing and quality assurance
- Code review and mentoring capabilities