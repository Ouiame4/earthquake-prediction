#!/usr/bin/env python3
"""
Rate Limiter avec Circuit Breaker conforme au projet
"""
import time
from datetime import datetime, timedelta
from functools import wraps

class CircuitBreaker:
    """Circuit Breaker pour protéger contre les API failures"""
    
    def __init__(self, failure_threshold=5, timeout=60, name="API"):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.name = name
    
    def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        
        if self.state == "OPEN":
            if datetime.now() - self.last_failure_time > timedelta(seconds=self.timeout):
                print(f"🔄 {self.name} Circuit Breaker: OPEN → HALF_OPEN")
                self.state = "HALF_OPEN"
            else:
                raise Exception(f"❌ {self.name} Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self.on_success()
            return result
        except Exception as e:
            self.on_failure()
            raise e
    
    def on_success(self):
        """Reset after successful call"""
        if self.state == "HALF_OPEN":
            print(f"✅ {self.name} Circuit Breaker: HALF_OPEN → CLOSED")
        self.failure_count = 0
        self.state = "CLOSED"
    
    def on_failure(self):
        """Handle failure"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.failure_count >= self.failure_threshold:
            print(f"🚨 {self.name} Circuit Breaker: CLOSED → OPEN (failures: {self.failure_count})")
            self.state = "OPEN"
        else:
            print(f"⚠️  {self.name} Failure {self.failure_count}/{self.failure_threshold}")

class RateLimiter:
    """Rate Limiter decorator"""
    
    def __init__(self, max_calls, period, name="API"):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.name = name
    
    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            now = time.time()
            
            # Nettoyer les anciens appels
            self.calls = [call for call in self.calls if now - call < self.period]
            
            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now - self.calls[0])
                print(f"⏳ {self.name} Rate limit ({self.max_calls}/{self.period}s), pause de {sleep_time:.1f}s")
                time.sleep(sleep_time)
                self.calls = []
            
            self.calls.append(now)
            return func(*args, **kwargs)
        
        return wrapper

def exponential_backoff(func, max_retries=5, base_delay=1):
    """Retry with exponential backoff"""
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                print(f"🔄 Retry {attempt + 1}/{max_retries} après {delay}s: {e}")
                time.sleep(delay)
            else:
                raise e

# Export
__all__ = ['CircuitBreaker', 'RateLimiter', 'exponential_backoff']
